
use axum::{routing::get, Router};
use clap::Parser;
use futures::StreamExt;
use hdrhistogram::Histogram;
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

static METRICS: Lazy<Arc<Mutex<Metrics>>> = Lazy::new(|| Arc::new(Mutex::new(Metrics::default())));

struct Metrics {
    trades: u64,
    decisions: u64,
    fills: u64,
    pnl: f64,
    last_price: f64,
    // latency from trade timestamp to decision time (ms)
    lat_hist: Histogram<u64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            trades: 0,
            decisions: 0,
            fills: 0,
            pnl: 0.0,
            last_price: 0.0,
            lat_hist: Histogram::<u64>::new(3).unwrap(),
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(name = "quant-mini")]
#[command(about = "Binance WS -> MA strategy -> paper trading -> metrics")]
struct Args {
    /// Trading symbol, e.g. btcusdt
    #[arg(long, default_value = "btcusdt")]
    symbol: String,

    /// Moving average window size (number of trades)
    #[arg(long, default_value_t = 50)]
    ma_window: usize,

    /// Threshold in basis points (e.g. 10 = 0.1%) to trigger entry/exit
    #[arg(long, default_value_t = 10)]
    threshold_bps: u32,

    /// Metrics server port
    #[arg(long, default_value_t = 9000)]
    metrics_port: u16,
}

#[derive(Debug, Deserialize, Clone)]
struct AggTrade {
    e: String,
    E: u64,
    s: String,
    a: u64,
    p: String,
    q: String,
    T: u64,
    m: bool,
    #[allow(dead_code)]
    M: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let symbol = args.symbol.to_lowercase();
    let stream_url = format!("wss://stream.binance.com:9443/ws/{}@aggTrade", symbol);
    println!("Connecting to: {}", &stream_url);

    // spawn metrics server
    let metrics_app = Router::new().route("/metrics", get(metrics_handler));
    let metrics_port = args.metrics_port;
    tokio::spawn(async move {
        let addr: std::net::SocketAddr = format!("0.0.0.0:{metrics_port}").parse().unwrap();
        println!("Metrics on http://{addr}/metrics");
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, metrics_app).await.unwrap();
    });

    let (tx, mut rx) = mpsc::channel::<AggTrade>(4096);

    // WS reader task
    let url = url::Url::parse(&stream_url)?;
    tokio::spawn(async move {
        let (ws_stream, _) = connect_async(url).await.expect("WS connect");
        println!("WebSocket connected");
        let (_write, mut read) = ws_stream.split();
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(txt)) => {
                    // Binance sometimes wraps into {stream, data}, sometimes direct (for /ws)
                    let de: serde_json::Value = serde_json::from_str(&txt).unwrap_or_default();
                    let trade_opt = if de.get("data").is_some() {
                        serde_json::from_value::<AggTrade>(de.get("data").unwrap().clone()).ok()
                    } else {
                        serde_json::from_value::<AggTrade>(de.clone()).ok()
                    };
                    if let Some(t) = trade_opt {
                        let _ = tx.try_send(t);
                    }
                }
                Ok(Message::Binary(_)) => {}
                Ok(Message::Ping(_)) => {}
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    eprintln!("WS error: {e}");
                    break;
                }
            }
        }
        eprintln!("WS reader ended");
    });

    // Strategy + paper trader
    let mut prices: Vec<f64> = Vec::new();
    let mut pos_qty: f64 = 0.0;
    let mut cash: f64 = 0.0;

    while let Some(tr) = rx.recv().await {
        let price: f64 = tr.p.parse().unwrap_or(0.0);
        let _qty: f64 = tr.q.parse().unwrap_or(0.0);
        prices.push(price);
        if prices.len() > args.ma_window {
            prices.remove(0);
        }

        // metrics update
        {
            let mut m = METRICS.lock().unwrap();
            m.trades += 1;
            m.last_price = price;
        }

        if prices.len() < args.ma_window {
            continue;
        }
        let ma: f64 = prices.iter().sum::<f64>() / prices.len() as f64;
        let up = ma * (1.0 + args.threshold_bps as f64 / 10000.0);
        let dn = ma * (1.0 - args.threshold_bps as f64 / 10000.0);

        let mut decision: Option<&'static str> = None;
        if pos_qty <= 0.0 && price > up {
            decision = Some("BUY");
        } else if pos_qty > 0.0 && price < dn {
            decision = Some("SELL");
        }

        if let Some(side) = decision {
            // latency: now - trade time
            let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let latency = now_ms.saturating_sub(tr.T);
            {
                let mut m = METRICS.lock().unwrap();
                m.decisions += 1;
                let _ = m.lat_hist.record(latency);
            }
            // paper fill at current price
            match side {
                "BUY" => {
                    pos_qty = 1.0;
                    cash -= price * pos_qty;
                }
                "SELL" => {
                    cash += price * pos_qty;
                    pos_qty = 0.0;
                }
                _ => {}
            }
            let equity = cash + pos_qty * price;
            {
                let mut m = METRICS.lock().unwrap();
                m.fills += 1;
                m.pnl = equity; // start from 0 cash, equity equals PnL
            }
            println!("[{}] price={:.2} ma={:.2} -> {} | equity={:.2}", tr.T, price, ma, side, equity);
        }
    }

    Ok(())
}

async fn metrics_handler() -> String {
    let m = METRICS.lock().unwrap();
    let p50 = m.lat_hist.value_at_quantile(0.50);
    let p90 = m.lat_hist.value_at_quantile(0.90);
    let p99 = m.lat_hist.value_at_quantile(0.99);
    format!(
        concat!(
        "# HELP quant_trades_total Number of trades processed\n",
        "# TYPE quant_trades_total counter\n",
        "quant_trades_total {}\n",
        "# HELP quant_decisions_total Decisions made by strategy\n",
        "# TYPE quant_decisions_total counter\n",
        "quant_decisions_total {}\n",
        "# HELP quant_fills_total Paper fills\n",
        "# TYPE quant_fills_total counter\n",
        "quant_fills_total {}\n",
        "# HELP quant_pnl Equity value as PnL baseline\n",
        "# TYPE quant_pnl gauge\n",
        "quant_pnl {}\n",
        "# HELP quant_last_price Last trade price\n",
        "# TYPE quant_last_price gauge\n",
        "quant_last_price {}\n",
        "# HELP quant_latency_ms Decision latency histogram (p50/p90/p99)\n",
        "# TYPE quant_latency_ms summary\n",
        "quant_latency_ms{{quantile=\"0.50\"}} {}\n",
        "quant_latency_ms{{quantile=\"0.90\"}} {}\n",
        "quant_latency_ms{{quantile=\"0.99\"}} {}\n",
        ),
        m.trades, m.decisions, m.fills, m.pnl, m.last_price, p50, p90, p99
    )
}

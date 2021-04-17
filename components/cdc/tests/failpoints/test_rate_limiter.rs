// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use cdc::rate_limiter::testing_util::*;
use cdc::rate_limiter::*;
use std::thread::sleep;
use tokio::stream::StreamExt;

type MockCdcEvent = u64;

fn make_run_time() -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new();
    builder
        .threaded_scheduler()
        .core_threads(16)
        .build()
        .unwrap()
}

#[test]
fn test_rate_limiter_blocking() {
    let mut runtime = make_run_time();
    let mut runtime_1 = make_run_time();

    let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1, 1024);
    let (sink, mut rx) = futures::channel::mpsc::channel(1);
    let drain_handle = runtime_1.spawn(async move {
        let ret = drainer.drain(sink, ()).await;
        ret
    });

    let rate_limiter_clone = rate_limiter.clone();
    fail::cfg("cdc_rate_limiter_scan_prepare_to_block", "pause").unwrap();
    let send_handle = runtime.spawn(async move {
        rate_limiter_clone.send_scan_event(0).await.unwrap();
        rate_limiter_clone.send_scan_event(1).await.unwrap();
        rate_limiter_clone.send_scan_event(2).await.unwrap();
        rate_limiter_clone.start_flush();
    });

    let unblock_handle = runtime_1.spawn(async move {
        tokio::time::delay_for(std::time::Duration::from_millis(2000)).await;
        fail::cfg("cdc_rate_limiter_scan_prepare_to_block", "off").unwrap();
    });

    runtime_1.block_on(async move {
        assert_eq!(rx.next().await.unwrap().0, 0);
        assert_eq!(rx.next().await.unwrap().0, 1);
        assert_eq!(rx.next().await.unwrap().0, 2);

        std::mem::drop(rate_limiter);
        rx.close();

        unblock_handle.await.unwrap();
        send_handle.await.unwrap();
        drain_handle.await.unwrap();
    })
}

#[test]
fn test_rpc_sink_error_wake_up_senders() {
    let mut runtime = make_run_time();
    let mut runtime1 = make_run_time();

    fail::cfg("cdc_rate_limiter_on_drainer_error", "pause").unwrap();

    let (rate_limiter, drainer) = new_pair::<MockCdcEvent>(1, 1024);
    let mock_sink = MockRpcSink::new();
    let drain_handle = runtime1.spawn(drainer.drain(mock_sink.clone(), ()));
    let sender_handle = runtime.spawn(async move {
        rate_limiter.send_scan_event(0).await.unwrap();
        rate_limiter.send_scan_event(1).await.unwrap();
        assert_eq!(
            rate_limiter.send_scan_event(2).await,
            Err(RateLimiterError::DisconnectedError)
        );
    });

    sleep(std::time::Duration::from_millis(1000));
    mock_sink.inject_send_error(MockRpcError::InjectedRpcError);
    sleep(std::time::Duration::from_millis(5000));
    fail::cfg("cdc_rate_limiter_on_drainer_error", "off").unwrap();

    sleep(std::time::Duration::from_millis(5000));
    runtime1.block_on(async move {
        let res = drain_handle.await.unwrap();
        assert_eq!(
            res,
            Err(DrainerError::RpcSinkError(MockRpcError::InjectedRpcError))
        );
    });
}

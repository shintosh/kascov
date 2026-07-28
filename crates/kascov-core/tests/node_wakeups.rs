use std::time::Duration;

use kascov_core::node::{chain_wakeup_channel, ChainWakeupKind};

#[tokio::test]
async fn notification_maps_to_a_stable_observation_time() {
    let (publisher, mut wakeups) = chain_wakeup_channel();
    publisher.publish(ChainWakeupKind::VirtualChainChanged, 1_234);

    let wakeup = wakeups.recv().await.expect("notification wakeup");
    assert_eq!(ChainWakeupKind::VirtualChainChanged, wakeup.kind);
    assert_eq!(1_234, wakeup.observed_at_ms);
}

#[tokio::test]
async fn disconnect_is_a_distinct_wakeup() {
    let (publisher, mut wakeups) = chain_wakeup_channel();
    publisher.publish(ChainWakeupKind::Disconnected, 2_345);

    let wakeup = wakeups.recv().await.expect("disconnect wakeup");
    assert_eq!(ChainWakeupKind::Disconnected, wakeup.kind);
    assert_eq!(2_345, wakeup.observed_at_ms);
}

#[tokio::test]
async fn unread_notifications_coalesce_to_the_latest_observation() {
    let (publisher, mut wakeups) = chain_wakeup_channel();
    publisher.publish(ChainWakeupKind::VirtualChainChanged, 10);
    publisher.publish(ChainWakeupKind::VirtualChainChanged, 20);
    publisher.publish(ChainWakeupKind::VirtualChainChanged, 30);

    let wakeup = wakeups.recv().await.expect("coalesced wakeup");
    assert_eq!(30, wakeup.observed_at_ms);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), wakeups.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_silent_adapter_produces_no_wakeup() {
    let (_publisher, mut wakeups) = chain_wakeup_channel();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), wakeups.recv())
            .await
            .is_err()
    );
}

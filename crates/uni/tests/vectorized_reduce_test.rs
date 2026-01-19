// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni::Uni;

#[tokio::test]
async fn test_reduce_vectorized() {
    let _ = std::fs::remove_dir_all("tmp/test_reduce_vectorized");
    let uni = Uni::open("tmp/test_reduce_vectorized")
        .build()
        .await
        .unwrap();
    uni.execute("CREATE LABEL Item (vals JSON)").await.unwrap();
    uni.execute("CREATE (:Item {vals: [1, 2, 3, 4, 5]})")
        .await
        .unwrap();
    uni.flush().await.unwrap();

    let res = uni
        .query("MATCH (n:Item) RETURN reduce(acc = 0, x IN n.vals | acc + x) AS sum")
        .await
        .unwrap();

    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0].get::<i64>("sum").unwrap(), 15);
}

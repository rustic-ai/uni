// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni::Uni;

#[tokio::test]
async fn test_window_row_number() {
    let _ = std::fs::remove_dir_all("tmp/test_window_row_number");
    let uni = Uni::open("tmp/test_window_row_number")
        .build()
        .await
        .unwrap();
    uni.execute("CREATE LABEL Item (val INTEGER)")
        .await
        .unwrap();
    uni.execute("CREATE (:Item {val: 10}), (:Item {val: 20}), (:Item {val: 30})")
        .await
        .unwrap();
    uni.flush().await.unwrap();

    let res = uni
        .query("MATCH (n:Item) RETURN n.val, row_number() OVER (ORDER BY n.val) as rn ORDER BY rn")
        .await
        .unwrap();

    assert_eq!(res.rows.len(), 3);
    assert_eq!(res.rows[0].get::<i64>("rn").unwrap(), 1);
    assert_eq!(res.rows[1].get::<i64>("rn").unwrap(), 2);
    assert_eq!(res.rows[2].get::<i64>("rn").unwrap(), 3);
}

#[tokio::test]
async fn test_window_rank() {
    let _ = std::fs::remove_dir_all("tmp/test_window_rank");
    let uni = Uni::open("tmp/test_window_rank").build().await.unwrap();
    uni.execute("CREATE LABEL Score (v INTEGER)").await.unwrap();
    uni.execute("CREATE (:Score {v: 10}), (:Score {v: 20}), (:Score {v: 20}), (:Score {v: 30})")
        .await
        .unwrap();
    uni.flush().await.unwrap();

    let res = uni
        .query("MATCH (n:Score) RETURN n.v, rank() OVER (ORDER BY n.v) as rk ORDER BY rk")
        .await
        .unwrap();

    // 10 -> 1
    // 20 -> 2
    // 20 -> 2
    // 30 -> 4

    let ranks: Vec<i64> = res
        .rows
        .iter()
        .map(|r| r.get::<i64>("rk").unwrap())
        .collect();
    assert_eq!(ranks, vec![1, 2, 2, 4]);
}

#[tokio::test]
async fn test_window_dense_rank() {
    let _ = std::fs::remove_dir_all("tmp/test_window_dense_rank");
    let uni = Uni::open("tmp/test_window_dense_rank")
        .build()
        .await
        .unwrap();
    uni.execute("CREATE LABEL Score (v INTEGER)").await.unwrap();
    uni.execute("CREATE (:Score {v: 10}), (:Score {v: 20}), (:Score {v: 20}), (:Score {v: 30})")
        .await
        .unwrap();
    uni.flush().await.unwrap();

    let res = uni
        .query("MATCH (n:Score) RETURN n.v, dense_rank() OVER (ORDER BY n.v) as rk ORDER BY rk")
        .await
        .unwrap();

    // 10 -> 1
    // 20 -> 2
    // 20 -> 2
    // 30 -> 3

    let ranks: Vec<i64> = res
        .rows
        .iter()
        .map(|r| r.get::<i64>("rk").unwrap())
        .collect();
    assert_eq!(ranks, vec![1, 2, 2, 3]);
}

#[tokio::test]
async fn test_window_partition() {
    let _ = std::fs::remove_dir_all("tmp/test_window_partition");
    let uni = Uni::open("tmp/test_window_partition")
        .build()
        .await
        .unwrap();
    uni.execute("CREATE LABEL Emp (dept STRING, sal INTEGER)")
        .await
        .unwrap();
    uni.execute("CREATE (:Emp {dept: 'A', sal: 100}), (:Emp {dept: 'A', sal: 200}), (:Emp {dept: 'B', sal: 150})").await.unwrap();
    uni.flush().await.unwrap();

    let res = uni.query("MATCH (n:Emp) RETURN n.dept, n.sal, row_number() OVER (PARTITION BY n.dept ORDER BY n.sal) as rn ORDER BY n.dept, rn").await.unwrap();

    // A, 100 -> 1
    // A, 200 -> 2
    // B, 150 -> 1

    let rns: Vec<i64> = res
        .rows
        .iter()
        .map(|r| r.get::<i64>("rn").unwrap())
        .collect();
    assert_eq!(rns, vec![1, 2, 1]);
}

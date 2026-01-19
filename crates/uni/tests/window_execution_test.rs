// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni::Uni;

#[tokio::test]
async fn test_window_row_number() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL Employee (dept STRING, salary INT)")
        .await?;

    // Dept A
    db.execute("CREATE (e:Employee {dept: 'A', salary: 100})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 200})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'A', salary: 300})")
        .await?;

    // Dept B
    db.execute("CREATE (e:Employee {dept: 'B', salary: 150})")
        .await?;
    db.execute("CREATE (e:Employee {dept: 'B', salary: 250})")
        .await?;

    // Query: ROW_NUMBER partitioned by dept, ordered by salary DESC
    // Expected:
    // A: 300 -> 1, 200 -> 2, 100 -> 3
    // B: 250 -> 1, 150 -> 2

    let result = db.query("MATCH (e:Employee) RETURN e.dept AS dept, e.salary AS salary, row_number() OVER (PARTITION BY e.dept ORDER BY e.salary DESC) AS rn ORDER BY e.dept, row_number() OVER (PARTITION BY e.dept ORDER BY e.salary DESC)").await?;

    assert_eq!(result.len(), 5);

    let rows = result.rows();

    // Row 0: Dept A, Salary 300, RN 1
    assert_eq!(rows[0].get::<String>("dept")?, "A");
    assert_eq!(rows[0].get::<i64>("salary")?, 300);
    assert_eq!(rows[0].get::<i64>("rn")?, 1);

    // Row 1: Dept A, Salary 200, RN 2
    assert_eq!(rows[1].get::<String>("dept")?, "A");
    assert_eq!(rows[1].get::<i64>("salary")?, 200);
    assert_eq!(rows[1].get::<i64>("rn")?, 2);

    // Row 2: Dept A, Salary 100, RN 3
    assert_eq!(rows[2].get::<String>("dept")?, "A");
    assert_eq!(rows[2].get::<i64>("salary")?, 100);
    assert_eq!(rows[2].get::<i64>("rn")?, 3);

    // Row 3: Dept B, Salary 250, RN 1
    assert_eq!(rows[3].get::<String>("dept")?, "B");
    assert_eq!(rows[3].get::<i64>("salary")?, 250);
    assert_eq!(rows[3].get::<i64>("rn")?, 1);

    // Row 4: Dept B, Salary 150, RN 2
    assert_eq!(rows[4].get::<String>("dept")?, "B");
    assert_eq!(rows[4].get::<i64>("salary")?, 150);
    assert_eq!(rows[4].get::<i64>("rn")?, 2);

    Ok(())
}

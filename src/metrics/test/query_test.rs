#[cfg(test)]
mod tests {
    use anyhow::Result;
    use sqlx::Error as SqlxError;

    /// downcast_ref를 사용한 sqlx::Error 복구 테스트
    #[test]
    fn test_downcast_sqlx_error() {
        // sqlx::Error를 anyhow::Error로 변환
        let sqlx_err = SqlxError::RowNotFound;
        let anyhow_err = anyhow::Error::from(sqlx_err);

        // downcast_ref로 원본 타입 복구
        let recovered = anyhow_err.downcast_ref::<SqlxError>();

        // 복구 성공 확인
        assert!(recovered.is_some());

        // RowNotFound 패턴 매칭 확인
        if let Some(err) = recovered {
            assert!(matches!(err, SqlxError::RowNotFound));
        }
    }

    /// 다른 sqlx::Error 타입도 복구 가능한지 테스트
    #[test]
    fn test_downcast_other_sqlx_errors() {
        // Database 에러
        let sqlx_err = SqlxError::Configuration("test config error".into());
        let anyhow_err = anyhow::Error::from(sqlx_err);

        let recovered = anyhow_err.downcast_ref::<SqlxError>();
        assert!(recovered.is_some());

        if let Some(err) = recovered {
            assert!(matches!(err, SqlxError::Configuration(_)));
        }
    }

    /// 타임아웃 에러는 downcast 실패하는지 테스트
    #[test]
    fn test_timeout_error_not_sqlx() {
        // 순수 anyhow 에러 (sqlx::Error가 아님)
        let timeout_err = anyhow::anyhow!("Query timeout after 1000ms");

        // downcast 실패 확인
        let recovered = timeout_err.downcast_ref::<SqlxError>();
        assert!(recovered.is_none());
    }

    /// 실제 사용 패턴 시뮬레이션
    #[test]
    fn test_error_handling_pattern() {
        fn simulate_query_with_row_not_found() -> Result<String> {
            // RowNotFound 에러 발생 시뮬레이션
            let err = SqlxError::RowNotFound;
            Err(anyhow::Error::from(err))
        }

        fn simulate_query_with_other_error() -> Result<String> {
            // 다른 에러 발생 시뮬레이션
            let err = SqlxError::Protocol("protocol error".into());
            Err(anyhow::Error::from(err))
        }

        fn handle_query_result(result: Result<String>) -> Result<String> {
            match result {
                Ok(data) => Ok(data),
                Err(e) => {
                    // downcast로 sqlx::Error 체크
                    if let Some(sqlx_err) = e.downcast_ref::<SqlxError>() {
                        if matches!(sqlx_err, SqlxError::RowNotFound) {
                            // 기본값 반환
                            return Ok("default_value".to_string());
                        }
                    }
                    // 다른 에러는 전파
                    Err(e)
                }
            }
        }

        // RowNotFound는 기본값 반환
        let result1 = handle_query_result(simulate_query_with_row_not_found());
        assert_eq!(result1.unwrap(), "default_value");

        // 다른 에러는 전파
        let result2 = handle_query_result(simulate_query_with_other_error());
        assert!(result2.is_err());
    }

    /// measure_postgres! 매크로 사용 예제 (실제 DB 없이 패턴 확인)
    #[tokio::test]
    async fn test_macro_pattern_example() {
        // 실제 매크로 사용 시뮬레이션
        async fn mock_query() -> std::result::Result<String, SqlxError> {
            Err(SqlxError::RowNotFound)
        }

        // 실제 코드에서 사용할 패턴
        let result = mock_query().await;

        match result {
            Ok(data) => {
                println!("Success: {}", data);
            }
            Err(e) => {
                // anyhow::Error로 변환 (measure_postgres! 매크로가 하는 일)
                let anyhow_err = anyhow::Error::from(e);

                // downcast로 원본 에러 체크
                if let Some(sqlx_err) = anyhow_err.downcast_ref::<SqlxError>() {
                    if matches!(sqlx_err, SqlxError::RowNotFound) {
                        println!("Row not found, using default");
                        // 기본값 사용
                        return;
                    }
                }

                panic!("Unexpected error: {}", anyhow_err);
            }
        }
    }

    /// 복합 에러 처리 시나리오
    #[test]
    fn test_complex_error_scenarios() {
        use std::collections::HashMap;

        // 다양한 sqlx::Error를 anyhow로 변환 후 복구
        let mut error_map = HashMap::new();

        // RowNotFound
        let err1 = anyhow::Error::from(SqlxError::RowNotFound);
        error_map.insert("row_not_found", err1);

        // Configuration
        let err2 = anyhow::Error::from(SqlxError::Configuration("config".into()));
        error_map.insert("configuration", err2);

        // Protocol
        let err3 = anyhow::Error::from(SqlxError::Protocol("protocol".into()));
        error_map.insert("protocol", err3);

        // 순수 anyhow (downcast 실패해야 함)
        let err4 = anyhow::anyhow!("Pure anyhow error");
        error_map.insert("pure_anyhow", err4);

        // 각 에러 타입 확인
        for (key, err) in error_map.iter() {
            match key {
                &"row_not_found" => {
                    let sqlx_err = err.downcast_ref::<SqlxError>().unwrap();
                    assert!(matches!(sqlx_err, SqlxError::RowNotFound));
                }
                &"configuration" => {
                    let sqlx_err = err.downcast_ref::<SqlxError>().unwrap();
                    assert!(matches!(sqlx_err, SqlxError::Configuration(_)));
                }
                &"protocol" => {
                    let sqlx_err = err.downcast_ref::<SqlxError>().unwrap();
                    assert!(matches!(sqlx_err, SqlxError::Protocol(_)));
                }
                &"pure_anyhow" => {
                    // downcast 실패해야 함
                    assert!(err.downcast_ref::<SqlxError>().is_none());
                }
                _ => panic!("Unknown error key"),
            }
        }
    }
}

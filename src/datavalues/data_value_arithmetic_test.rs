// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_data_value_arithmetic() {
    use super::*;

    #[allow(dead_code)]
    struct ScalarTest<'a> {
        name: &'static str,
        args: &'a Vec<Vec<DataValue>>,
        expect: Vec<DataValue>,
        error: Vec<&'static str>,
        op: DataValueArithmeticOperator,
    }

    let args = vec![
        // Error.
        vec![
            DataValue::String(Some("xx".to_string())),
            DataValue::Int8(Some(2)),
        ],
        // Float64.
        vec![DataValue::Float64(Some(3.0)), DataValue::Float64(Some(2.0))],
        vec![DataValue::Float64(Some(3.0)), DataValue::Float32(Some(2.0))],
        vec![DataValue::Float64(Some(3.0)), DataValue::Int64(Some(2))],
        vec![DataValue::Float64(Some(3.0)), DataValue::Int32(Some(2))],
        vec![DataValue::Float64(Some(3.0)), DataValue::Int16(Some(2))],
        vec![DataValue::Float64(Some(3.0)), DataValue::Int8(Some(2))],
        vec![DataValue::Float64(Some(3.0)), DataValue::UInt64(Some(2))],
        vec![DataValue::Float64(Some(3.0)), DataValue::UInt32(Some(2))],
        vec![DataValue::Float64(Some(3.0)), DataValue::UInt16(Some(2))],
        vec![DataValue::Float64(Some(3.0)), DataValue::UInt8(Some(2))],
        // Float32.
        vec![DataValue::Float32(Some(3.0)), DataValue::Float64(Some(2.0))],
        vec![DataValue::Float32(Some(3.0)), DataValue::Float32(Some(2.0))],
        vec![DataValue::Float32(Some(3.0)), DataValue::Int64(Some(2))],
        vec![DataValue::Float32(Some(3.0)), DataValue::Int32(Some(2))],
        vec![DataValue::Float32(Some(3.0)), DataValue::Int16(Some(2))],
        vec![DataValue::Float32(Some(3.0)), DataValue::Int8(Some(2))],
        vec![DataValue::Float32(Some(3.0)), DataValue::UInt64(Some(2))],
        vec![DataValue::Float32(Some(3.0)), DataValue::UInt32(Some(2))],
        vec![DataValue::Float32(Some(3.0)), DataValue::UInt16(Some(2))],
        vec![DataValue::Float32(Some(3.0)), DataValue::UInt8(Some(2))],
        // Int8.
        vec![DataValue::Int8(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::Int8(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::Int8(Some(3)), DataValue::Int64(Some(2))],
        vec![DataValue::Int8(Some(3)), DataValue::Int32(Some(2))],
        vec![DataValue::Int8(Some(3)), DataValue::Int16(Some(2))],
        vec![DataValue::Int8(Some(3)), DataValue::Int8(Some(2))],
        vec![DataValue::Int8(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::Int8(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::Int8(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::Int8(Some(3)), DataValue::UInt8(Some(2))],
        // UInt8.
        vec![DataValue::UInt8(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::UInt8(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::UInt8(Some(3)), DataValue::Int64(Some(2))],
        vec![DataValue::UInt8(Some(3)), DataValue::Int32(Some(2))],
        vec![DataValue::UInt8(Some(3)), DataValue::Int16(Some(2))],
        vec![DataValue::UInt8(Some(3)), DataValue::UInt8(Some(2))],
        vec![DataValue::UInt8(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::UInt8(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::UInt8(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::UInt8(Some(3)), DataValue::UInt8(Some(2))],
        // Int16.
        vec![DataValue::Int16(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::Int16(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::Int16(Some(3)), DataValue::Int64(Some(2))],
        vec![DataValue::Int16(Some(3)), DataValue::Int32(Some(2))],
        vec![DataValue::Int16(Some(3)), DataValue::Int16(Some(2))],
        vec![DataValue::Int16(Some(3)), DataValue::Int8(Some(2))],
        vec![DataValue::Int16(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::Int16(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::Int16(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::Int16(Some(3)), DataValue::UInt8(Some(2))],
        // UInt16.
        vec![DataValue::UInt16(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::UInt16(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::UInt16(Some(3)), DataValue::Int64(Some(2))],
        vec![DataValue::UInt16(Some(3)), DataValue::Int32(Some(2))],
        vec![DataValue::UInt16(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::UInt16(Some(3)), DataValue::Int8(Some(2))],
        vec![DataValue::UInt16(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::UInt16(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::UInt16(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::UInt16(Some(3)), DataValue::UInt8(Some(2))],
        // Int32.
        vec![DataValue::Int32(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::Int32(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::Int32(Some(3)), DataValue::Int64(Some(2))],
        vec![DataValue::Int32(Some(3)), DataValue::Int32(Some(2))],
        vec![DataValue::Int32(Some(3)), DataValue::Int16(Some(2))],
        vec![DataValue::Int32(Some(3)), DataValue::Int8(Some(2))],
        vec![DataValue::Int32(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::Int32(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::Int32(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::Int32(Some(3)), DataValue::UInt8(Some(2))],
        // UInt32.
        vec![DataValue::UInt32(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::UInt32(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::UInt32(Some(3)), DataValue::Int64(Some(2))],
        vec![DataValue::UInt32(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::UInt32(Some(3)), DataValue::Int16(Some(2))],
        vec![DataValue::UInt32(Some(3)), DataValue::Int8(Some(2))],
        vec![DataValue::UInt32(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::UInt32(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::UInt32(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::UInt32(Some(3)), DataValue::UInt8(Some(2))],
        // Int64.
        vec![DataValue::Int64(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::Int64(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::Int64(Some(3)), DataValue::Int64(Some(2))],
        vec![DataValue::Int64(Some(3)), DataValue::Int32(Some(2))],
        vec![DataValue::Int64(Some(3)), DataValue::Int16(Some(2))],
        vec![DataValue::Int64(Some(3)), DataValue::Int8(Some(2))],
        vec![DataValue::Int64(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::Int64(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::Int64(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::Int64(Some(3)), DataValue::UInt8(Some(2))],
        // UInt64.
        vec![DataValue::UInt64(Some(3)), DataValue::Float64(Some(2.0))],
        vec![DataValue::UInt64(Some(3)), DataValue::Float32(Some(2.0))],
        vec![DataValue::UInt64(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::UInt64(Some(3)), DataValue::Int32(Some(2))],
        vec![DataValue::UInt64(Some(3)), DataValue::Int16(Some(2))],
        vec![DataValue::UInt64(Some(3)), DataValue::Int8(Some(2))],
        vec![DataValue::UInt64(Some(3)), DataValue::UInt64(Some(2))],
        vec![DataValue::UInt64(Some(3)), DataValue::UInt32(Some(2))],
        vec![DataValue::UInt64(Some(3)), DataValue::UInt16(Some(2))],
        vec![DataValue::UInt64(Some(3)), DataValue::UInt8(Some(2))],
    ];

    let tests = vec![
        ScalarTest {
            name: "add-passed",
            args: &args,
            op: DataValueArithmeticOperator::Add,
            expect: vec![
                // Place Hold.
                DataValue::String(Some("xx".to_string())),
                // Float64.
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                DataValue::Float64(Some(5.0)),
                // Float64.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                // Int8.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Int64(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::Int16(Some(5)),
                DataValue::Int8(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt16(Some(5)),
                DataValue::UInt8(Some(5)),
                // UInt8.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Int64(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::Int16(Some(5)),
                DataValue::UInt8(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt16(Some(5)),
                DataValue::UInt8(Some(5)),
                // Int16.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Int64(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::Int16(Some(5)),
                DataValue::Int16(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt16(Some(5)),
                DataValue::Int16(Some(5)),
                // UInt16.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Int64(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::UInt16(Some(5)),
                DataValue::UInt16(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt16(Some(5)),
                DataValue::UInt16(Some(5)),
                // Int32.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Int64(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::Int32(Some(5)),
                DataValue::Int32(Some(5)),
                // UInt32.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Int64(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt32(Some(5)),
                DataValue::UInt32(Some(5)),
                // Int64.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::Int64(Some(5)),
                DataValue::Int64(Some(5)),
                DataValue::Int64(Some(5)),
                DataValue::Int64(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::Int64(Some(5)),
                DataValue::Int64(Some(5)),
                DataValue::Int64(Some(5)),
                // UInt64.
                DataValue::Float64(Some(5.0)),
                DataValue::Float32(Some(5.0)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt64(Some(5)),
                DataValue::UInt64(Some(5)),
            ],
            error: vec!["Internal Error: Unsupported data value operator: Utf8 + Int8"],
        },
        ScalarTest {
            name: "sub-passed",
            args: &args,
            op: DataValueArithmeticOperator::Sub,
            expect: vec![
                // Place Hold.
                DataValue::String(Some("xx".to_string())),
                // Float64.
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                DataValue::Float64(Some(1.0)),
                // Float64.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                // Int8.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Int64(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int16(Some(1)),
                DataValue::Int8(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt8(Some(1)),
                // UInt8.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Int64(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int16(Some(1)),
                DataValue::UInt8(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt8(Some(1)),
                // Int16.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Int64(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int16(Some(1)),
                DataValue::Int16(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::Int16(Some(1)),
                // UInt16.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Int64(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt16(Some(1)),
                // Int32.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Int64(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int32(Some(1)),
                // UInt32.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Int64(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt32(Some(1)),
                // Int64.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::Int64(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::Int64(Some(1)),
                // UInt64.
                DataValue::Float64(Some(1.0)),
                DataValue::Float32(Some(1.0)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::UInt64(Some(1)),
            ],
            error: vec!["Internal Error: Unsupported data value operator: Utf8 - Int8"],
        },
        ScalarTest {
            name: "mul-passed",
            args: &args,
            op: DataValueArithmeticOperator::Mul,
            expect: vec![
                // Place Hold.
                DataValue::String(Some("xx".to_string())),
                // Float64.
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                DataValue::Float64(Some(6.0)),
                // Float64.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                // Int8.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Int64(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::Int16(Some(6)),
                DataValue::Int8(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt16(Some(6)),
                DataValue::UInt8(Some(6)),
                // UInt8.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Int64(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::Int16(Some(6)),
                DataValue::UInt8(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt16(Some(6)),
                DataValue::UInt8(Some(6)),
                // Int16.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Int64(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::Int16(Some(6)),
                DataValue::Int16(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt16(Some(6)),
                DataValue::Int16(Some(6)),
                // UInt16.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Int64(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::UInt16(Some(6)),
                DataValue::UInt16(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt16(Some(6)),
                DataValue::UInt16(Some(6)),
                // Int32.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Int64(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::Int32(Some(6)),
                DataValue::Int32(Some(6)),
                // UInt32.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Int64(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt32(Some(6)),
                DataValue::UInt32(Some(6)),
                // Int64.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::Int64(Some(6)),
                DataValue::Int64(Some(6)),
                DataValue::Int64(Some(6)),
                DataValue::Int64(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::Int64(Some(6)),
                DataValue::Int64(Some(6)),
                DataValue::Int64(Some(6)),
                // UInt64.
                DataValue::Float64(Some(6.0)),
                DataValue::Float32(Some(6.0)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt64(Some(6)),
                DataValue::UInt64(Some(6)),
            ],
            error: vec!["Internal Error: Unsupported data value operator: Utf8 * Int8"],
        },
        ScalarTest {
            name: "div-passed",
            args: &args,
            op: DataValueArithmeticOperator::Div,
            expect: vec![
                // Place Hold.
                DataValue::String(Some("xx".to_string())),
                // Float64.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // Float64.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // Int8.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // UInt8.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // Int16.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // UInt16.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // Int32.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // UInt32.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // Int64.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                // UInt64.
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
                DataValue::Float64(Some(1.5)),
            ],
            error: vec!["Internal Error: Unsupported data value operator: Utf8 / Int8"],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = data_value_arithmetic_op(t.op.clone(), args[0].clone(), args[1].clone());
            match result {
                Ok(v) => assert_eq!(v, t.expect[i]),
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}

// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

use databend_common_exception::Result;
use databend_common_expression::types::boolean::TrueIdxIter;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Decimal256Type;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::Int8Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt16Type;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableField;
use databend_common_expression::SELECTIVITY_THRESHOLD;
use databend_storages_common_table_meta::meta::BlockStatistics;
use databend_storages_common_table_meta::meta::BlockStatisticsMeta;
use databend_storages_common_table_meta::meta::ColumnDistinctHLL;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Versioned;
use enum_dispatch::enum_dispatch;

use crate::io::TableMetaLocationGenerator;

pub struct BlockStatisticsState {
    pub(crate) data: Vec<u8>,
    // pub(crate) size: u64,
    pub(crate) location: Location,
    pub(crate) column_distinct_count: HashMap<ColumnId, usize>,
}

impl BlockStatisticsState {
    pub fn from_data_block(
        block_location: &Location,
        block: &DataBlock,
        ndv_columns_map: &BTreeMap<FieldIndex, TableField>,
    ) -> Result<Option<Self>> {
        let mut builder = BlockStatsBuilder::new(ndv_columns_map);
        builder.add_block(block)?;
        builder.finalize(block_location)
    }

    pub fn block_stats_meta(&self) -> BlockStatisticsMeta {
        BlockStatisticsMeta {
            location: self.location.clone(),
            size: self.data.len() as u64,
        }
    }
}

pub struct BlockStatsBuilder {
    builders: Vec<ColumnNDVBuilder>,
}

pub struct ColumnNDVBuilder {
    index: FieldIndex,
    field: TableField,
    builder: ColumnNDVEstimator,
}

impl BlockStatsBuilder {
    pub fn new(ndv_columns_map: &BTreeMap<FieldIndex, TableField>) -> BlockStatsBuilder {
        let mut builders = Vec::with_capacity(ndv_columns_map.len());
        for (index, field) in ndv_columns_map {
            let builder = create_column_ndv_estimator(&field.data_type().into());
            builders.push(ColumnNDVBuilder {
                index: *index,
                field: field.clone(),
                builder,
            });
        }
        BlockStatsBuilder { builders }
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        for column_builder in self.builders.iter_mut() {
            let entry = block.get_by_offset(column_builder.index);
            match entry {
                BlockEntry::Const(s, ..) => {
                    column_builder.builder.update_scalar(&s.as_ref());
                }
                BlockEntry::Column(col) => {
                    column_builder.builder.update_column(col);
                }
            }
        }
        Ok(())
    }

    pub fn finalize(self, block_location: &Location) -> Result<Option<BlockStatisticsState>> {
        if self.builders.is_empty() {
            return Ok(None);
        }

        let mut hlls = HashMap::with_capacity(self.builders.len());
        let mut column_distinct_count = HashMap::with_capacity(self.builders.len());
        for column_builder in self.builders {
            let column_id = column_builder.field.column_id();
            let distinct_count = column_builder.builder.finalize();
            let hll = column_builder.builder.hll();
            hlls.insert(column_id, hll);
            column_distinct_count.insert(column_id, distinct_count);
        }

        let location = TableMetaLocationGenerator::gen_block_stats_location_from_block_location(
            &block_location.0,
        );
        let block_stats = BlockStatistics::new(hlls);
        let data = block_stats.to_bytes()?;
        Ok(Some(BlockStatisticsState {
            data,
            location: (location, BlockStatistics::VERSION),
            column_distinct_count,
        }))
    }
}

#[enum_dispatch]
pub trait ColumnNDVEstimatorOps: Send + Sync {
    fn update_column(&mut self, column: &Column);
    fn update_scalar(&mut self, scalar: &ScalarRef);
    fn finalize(&self) -> usize;
    fn hll(self) -> ColumnDistinctHLL;
}

#[enum_dispatch(ColumnNDVEstimatorOps)]
pub enum ColumnNDVEstimator {
    Int8(ColumnNDVEstimatorImpl<Int8Type>),
    Int16(ColumnNDVEstimatorImpl<Int16Type>),
    Int32(ColumnNDVEstimatorImpl<Int32Type>),
    Int64(ColumnNDVEstimatorImpl<Int64Type>),
    UInt8(ColumnNDVEstimatorImpl<UInt8Type>),
    UInt16(ColumnNDVEstimatorImpl<UInt16Type>),
    UInt32(ColumnNDVEstimatorImpl<UInt32Type>),
    UInt64(ColumnNDVEstimatorImpl<UInt64Type>),
    Float32(ColumnNDVEstimatorImpl<Float32Type>),
    Float64(ColumnNDVEstimatorImpl<Float64Type>),
    String(ColumnNDVEstimatorImpl<StringType>),
    Date(ColumnNDVEstimatorImpl<DateType>),
    Timestamp(ColumnNDVEstimatorImpl<TimestampType>),
    Decimal64(ColumnNDVEstimatorImpl<Decimal64Type>),
    Decimal128(ColumnNDVEstimatorImpl<Decimal128Type>),
    Decimal256(ColumnNDVEstimatorImpl<Decimal256Type>),
}

pub fn create_column_ndv_estimator(data_type: &DataType) -> ColumnNDVEstimator {
    macro_rules! match_number_type_create {
        ($inner_type:expr) => {{
            with_number_type!(|NUM_TYPE| match $inner_type {
                NumberDataType::NUM_TYPE => {
                    paste::paste! {
                        ColumnNDVEstimator::NUM_TYPE(ColumnNDVEstimatorImpl::<[<NUM_TYPE Type>]>::new())
                    }
                }
            })
        }};
    }

    let inner_type = data_type.remove_nullable();
    match inner_type {
        DataType::Number(num_type) => {
            match_number_type_create!(num_type)
        }
        DataType::String => ColumnNDVEstimator::String(ColumnNDVEstimatorImpl::<StringType>::new()),
        DataType::Date => ColumnNDVEstimator::Date(ColumnNDVEstimatorImpl::<DateType>::new()),
        DataType::Timestamp => {
            ColumnNDVEstimator::Timestamp(ColumnNDVEstimatorImpl::<TimestampType>::new())
        }
        DataType::Decimal(size) => {
            if size.can_carried_by_64() {
                ColumnNDVEstimator::Decimal64(ColumnNDVEstimatorImpl::<Decimal64Type>::new())
            } else if size.can_carried_by_128() {
                ColumnNDVEstimator::Decimal128(ColumnNDVEstimatorImpl::<Decimal128Type>::new())
            } else {
                ColumnNDVEstimator::Decimal256(ColumnNDVEstimatorImpl::<Decimal256Type>::new())
            }
        }
        _ => unreachable!("Unsupported data type: {:?}", data_type),
    }
}

pub struct ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    hll: ColumnDistinctHLL,
    _phantom: PhantomData<T>,
}

impl<T> ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    pub fn new() -> Self {
        Self {
            hll: ColumnDistinctHLL::new(),
            _phantom: Default::default(),
        }
    }
}

impl<T> ColumnNDVEstimatorOps for ColumnNDVEstimatorImpl<T>
where
    T: ValueType + Send + Sync,
    for<'a> T::ScalarRef<'a>: Hash,
{
    fn update_column(&mut self, column: &Column) {
        let (column, validity) = match column {
            Column::Nullable(box inner) => {
                let validity = if inner.validity.null_count() == 0 {
                    None
                } else {
                    Some(&inner.validity)
                };
                (&inner.column, validity)
            }
            Column::Null { .. } => return,
            column => (column, None),
        };

        let column = T::try_downcast_column(column).unwrap();
        if let Some(v) = validity {
            if v.true_count() as f64 / v.len() as f64 >= SELECTIVITY_THRESHOLD {
                for (data, valid) in T::iter_column(&column).zip(v.iter()) {
                    if valid {
                        self.hll.add_object(&data);
                    }
                }
            } else {
                TrueIdxIter::new(v.len(), Some(v)).for_each(|idx| {
                    let val = unsafe { T::index_column_unchecked(&column, idx) };
                    self.hll.add_object(&val);
                })
            }
        } else {
            for value in T::iter_column(&column) {
                self.hll.add_object(&value);
            }
        }
    }

    fn update_scalar(&mut self, scalar: &ScalarRef) {
        if matches!(scalar, ScalarRef::Null) {
            return;
        }

        let val = T::try_downcast_scalar(scalar).unwrap();
        self.hll.add_object(&val);
    }

    fn finalize(&self) -> usize {
        self.hll.count()
    }

    fn hll(self) -> ColumnDistinctHLL {
        self.hll
    }
}

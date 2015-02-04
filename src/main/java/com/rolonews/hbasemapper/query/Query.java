package com.rolonews.hbasemapper.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.rolonews.hbasemapper.*;
import com.rolonews.hbasemapper.annotations.Column;
import com.rolonews.hbasemapper.exceptions.ColumnNotMappedException;
import com.rolonews.hbasemapper.mapping.CellDescriptor;
import com.rolonews.hbasemapper.mapping.EntityMapper;
import com.rolonews.hbasemapper.mapping.MappingRegistry;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.lang.reflect.Field;
import java.util.Map;

/**
 *
 * Created by Sleiman on 14/12/2014.
 */
public class Query<T> implements IQuery<T>{

    private final Class<T> clazz;
    private final Scan scanner;

    private Query(Builder<T> builder) {
        this.clazz = builder.clazz;
        this.scanner = builder.scanner;
    }

    public static <T> Builder<T> builder(Class<T> clazz){
        return new Builder<T>(clazz);
    }

    @Override
    public Scan getScanner() {
        return this.scanner;
    }

    @Override
    public Class<T> getType() {
        return this.clazz;
    }

    public static class Builder<T> {

        private final Class<T> clazz;
        private final Scan scanner;
        private final ObjectSerializer serializer;
        private final FilterList filterList;
        private final EntityMapper<T> typeInfo;

        protected Builder(Class<T> clazz) {
            this.clazz = clazz;
            this.scanner = new Scan();
            this.filterList = new FilterList();
            this.serializer = new BasicObjectSerializer();
            this.typeInfo = MappingRegistry.registerIfAbsent(clazz);

        }

        public Builder<T> rowKeyPrefix(Object value) {
            byte[] prefix = serializer.serialize(value);
            Filter filter = new PrefixFilter(prefix);
            filterList.addFilter(filter);
            return this;
        }

        public Builder<T> startRow(Object startRow) {
            byte[] startRowBytes = serializer.serialize(startRow);
            scanner.setStartRow(startRowBytes);

            return this;
        }

        public Builder<T> limit(long limit){
            scanner.setMaxResultSize(limit);
            return this;
        }

        public Builder<T> stopRow(Object stopRow) {
            byte[] stopRowBytes = serializer.serialize(stopRow);
            scanner.setStopRow(stopRowBytes);

            return this;
        }

        public Builder<T> equals(final String field, Object value) {
            Preconditions.checkNotNull(field);
            Preconditions.checkNotNull(value);
            Pair<byte[], byte[]> familyQualPair = getColumnByFieldName(field, this.typeInfo);
            byte[] valueBytes = serializer.serialize(value);
            Filter filter = getComparisionFilter(familyQualPair, CompareFilter.CompareOp.EQUAL, valueBytes);
            this.filterList.addFilter(filter);

            return this;
        }

        public Builder<T> notEquals(final String field, Object value) {
            Preconditions.checkNotNull(field);
            Preconditions.checkNotNull(value);
            Pair<byte[], byte[]> familyQualPair = getColumnByFieldName(field, this.typeInfo);
            byte[] valueBytes = serializer.serialize(value);
            Filter filter = getComparisionFilter(familyQualPair, CompareFilter.CompareOp.NOT_EQUAL, valueBytes);
            this.filterList.addFilter(filter);

            return this;
        }

        public Builder<T> greaterThan(final String field, Comparable<?> value) {

            Preconditions.checkNotNull(field);
            Preconditions.checkNotNull(value);

            Pair<byte[], byte[]> familyQualPair = getColumnByFieldName(field, this.typeInfo);
            byte[] valueBytes = serializer.serialize(value);
            Filter filter = getComparisionFilter(familyQualPair, CompareFilter.CompareOp.GREATER, valueBytes);
            this.filterList.addFilter(filter);

            return this;
        }

        public Builder<T> greaterThanOrEqaul(final String field, Comparable<?> value) {
            Preconditions.checkNotNull(field);
            Preconditions.checkNotNull(value);

            Pair<byte[], byte[]> familyQualPair = getColumnByFieldName(field, this.typeInfo);
            byte[] valueBytes = serializer.serialize(value);
            Filter filter = getComparisionFilter(familyQualPair, CompareFilter.CompareOp.GREATER_OR_EQUAL, valueBytes);
            this.filterList.addFilter(filter);

            return this;
        }

        public Builder<T> lessThan(final String field, Comparable<?> value) {
            Preconditions.checkNotNull(field);
            Preconditions.checkNotNull(value);

            Pair<byte[], byte[]> familyQualPair = getColumnByFieldName(field, this.typeInfo);
            byte[] valueBytes = serializer.serialize(value);
            Filter filter = getComparisionFilter(familyQualPair, CompareFilter.CompareOp.LESS, valueBytes);
            this.filterList.addFilter(filter);

            return this;
        }

        public Builder<T> lessThanOrEqual(final String field, Comparable<?> value) {
            Preconditions.checkNotNull(field);
            Preconditions.checkNotNull(value);

            Pair<byte[], byte[]> familyQualPair = getColumnByFieldName(field, this.typeInfo);
            byte[] valueBytes = serializer.serialize(value);
            Filter filter = getComparisionFilter(familyQualPair, CompareFilter.CompareOp.LESS_OR_EQUAL, valueBytes);
            this.filterList.addFilter(filter);

            return this;
        }

        public Query<T> build() {
            this.scanner.setFilter(this.filterList);

            return new Query<T>(this);
        }

        private Pair<byte[], byte[]> getColumnByFieldName(final String field, EntityMapper<?> typeInfo) {

            Map<CellDescriptor, Field> columnFieldMap = Maps.filterKeys(typeInfo.columns(), new Predicate<CellDescriptor>() {
                @Override
                public boolean apply(CellDescriptor column) {
                    return column.qualifier().equals(field);
                }
            });

            if (columnFieldMap.size() == 0) {
                throw new ColumnNotMappedException("field of name " + field + "is not mapped, make sure you map your entity properly");
            }
            CellDescriptor column = Iterables.getLast(columnFieldMap.keySet());
            return new Pair<byte[], byte[]>(Bytes.toBytes(column.family()), Bytes.toBytes(column.qualifier()));
        }

        private SingleColumnValueFilter getComparisionFilter(Pair<byte[], byte[]> familyQualPair, CompareFilter.CompareOp compareOp, byte[] value) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(familyQualPair.getFirst(), familyQualPair.getSecond(), compareOp, value);
            return filter;
        }
    }

}

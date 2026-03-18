import polars as pl

pl.Config.set_tbl_cols(-1)

from pymongoarrow.api import find_arrow_all


class Requests:

    def __init__(self):
        pass

    def read_db(self, collection):

        arrow_table = find_arrow_all(collection, {})
        df = pl.DataFrame(arrow_table)

        return df

    def mean_reservation(self, df, availability_365, property_type):

        df = df.with_columns((365 - pl.col(availability_365)).alias("reserved_day"))

        df = df.with_columns((pl.col("reserved_day") / 365).alias("reservation_rate"))

        q = (
            df.lazy()
            .group_by(property_type)
            .agg(pl.col("reservation_rate").mean().round(2).alias("reservation_rate"))
            .sort("reservation_rate", descending=True)
        )

        return q.collect()

    def describe_review(self, df, review_col):

        return df.select(
            [
                pl.col(review_col).count().alias("count"),
                pl.col(review_col).null_count().alias("null"),
                pl.col(review_col).mean().alias("mean"),
                pl.col(review_col).std().alias("std"),
                pl.col(review_col).min().alias("min"),
                pl.col(review_col).quantile(0.25).alias("q025"),
                pl.col(review_col).median().alias("median"),
                pl.col(review_col).quantile(0.75).alias("q075"),
                pl.col(review_col).max().alias("max"),
            ]
        )

    def median_review_per_host_type(self, df, review_col, host_type_col):

        df = df.with_columns(pl.col(host_type_col).replace("", None))

        df = df.with_columns(pl.col(host_type_col).fill_null("f"))

        q = (
            df.lazy()
            .group_by(host_type_col)
            .agg(pl.col(review_col).median().round(2).alias("median_per_host_type"))
            .sort("median_per_host_type", descending=True)
        )

        return q.collect()

    def density(self, df, host_neighbourhood):

        q = (
            df.lazy()
            .group_by(host_neighbourhood)
            .agg(pl.col("id").count().round(2).alias("number_of_accommodation"))
            .sort("number_of_accommodation", descending=True)
        )

        return q.collect()

    def reservation_rate(self, df, availability_365, host_neighbourhood):

        df = df.with_columns((365 - pl.col(availability_365)).alias("reserved_day"))

        df = df.with_columns((pl.col("reserved_day") / 365).alias("reservation_rate"))

        q = (
            df.lazy()
            .group_by(host_neighbourhood)
            .agg(pl.col("reservation_rate").mean().round(2).alias("reservation_rate"))
            .sort("reservation_rate", descending=True)
        )

        return q.collect()

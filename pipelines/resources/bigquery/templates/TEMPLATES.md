# Jinja Templates for Recursive Transformations

Jinja Templates for Recursive Transformations are designed to generate queries that encapsulate best practices for efficiently executing common ETL tasks against existing tables. These templates are particularly useful for handling complex data transformations and aggregations.

Currently, our system supports two types of templates:

    1. Static Aggregations
    2. Rolling Aggregations

Each template has a required set of fields in order to render.

## At a Glance

* [Static Aggregations](#static-aggregations):
  * Fixed window aggregations.
  * One column per aggregation and aggregation window.
  * Returns the last value when the time period for the aggregation has lapsed.
  * Valid only if the window is complete.
  * Requires a [reference map](#reference-map).
* [Rolling Aggregations](#rolling-aggregations):
  * Sliding window style aggregations.
  * One column per aggregation and window length.
  * Each row represents some aggregation of a certain time interval.
  * Valid only if the entire rolling window has completed.
* [Reference Map](#reference-map):
  * Maps each primary key to a day (named `reference_day`) from which aggregations should be calculated.
  * Useful for tracking specific attributes or metrics associated with each primary key.

## Static Aggregations

Static Aggregations are designed to handle one column per aggregation or aggregation window. The key characteristic of this type of aggregation is that it always returns the last value when the time period for the aggregation has lapsed. However, the aggregation is only valid if the window is complete; otherwise, it returns Null. Because this template depends on complete aggregation windows, it's important to note that these aggregations must start at the minimum `reference_day` across all primary keys for backfills.

### Static Aggregations Example

If we consider the first week of a user's viewing history, a static aggregation would provide a summary of the user's activities during that week.

### Static Aggregations Required Fields

* `run_day` **str**: day of the current run of the template
* `start_date` **str**: the first day the template is expected to run.
* `features` **list[str]**: list containing the names of each feature to aggregate.
* `aggregations` **list[str]**: list of aggregations to perform.
* `aggregation_windows` **list[int]**: period to aggregate in combination with window length.
* `window_type` **str**: type of aggregation window, e.g. day.
* `window_length` **int**: length of the `aggregation_window`.
* `upstream_table` **str**: path to the table from which the aggregations are generated.
* `destination_table_id` **str**: the destination the template will write to.
* `reference_map` **str**: sql query to be used as a cte to generate the reference map for the aggregation.

`aggregation_windows`, `window_type`, `window_length` example:
An `aggregation_windows` of `2` with a `window_length` of `7` and a `window_type` of `day` would aggregate if and only if it is the second 7 day period after the `reference_day`.

## Rolling Aggregations

Rolling Aggregations follow a typical sliding window style. Each aggregation or window length is represented by one column. Each row in the aggregation represents some aggregation of a certain time interval, also known as 'n' previous rows. Similar to static aggregations, the aggregation is only valid if the entire rolling window has completed; otherwise, it returns Null.

### Rolling Aggregations Example

If we consider the last seven days of a user's viewing history, a rolling aggregation would provide a summary of the user's activities over the past seven days.

### Rolling Aggregations Required Fields

* `run_day` **str**: day of the current run of the template
* `start_date` **str**: the first day the template is expected to run.
* `features` **list[str]**: list containing the names of each feature to aggregate.
* `aggregations` **list[str]**: list of aggregations to perform.
* `window_type` **str**: type of aggregation window, e.g. day.
* `window_lengths` **list[int]**: length of each rolling aggregation to perform.
* `upstream_table` **str**: path to the table from which the aggregations are generated.
* `destination_table_id` **str**: the destination the template will write to.

`window_lengths`, `window_type` example:
A `window_length` of `7` and a `window_type` of `day` will generate rolling aggregations for the last seven days of the `feature`.

## Reference Map

Some templates require a **reference map**. A **reference map** is a tool that maps each primary key to a day from which aggregations should be calculated. This is particularly useful when you need to track specific attributes or metrics associated with each primary key. A **reference map** is defined using a query that returns data with the following schema.

### Reference Map Schema

* `day` **DATE**: day the record was created.
* `{primary key}` **STRING**: primary key for the reference day. Currently, only `adobe_tracking_id` is supported.
* `reference_day` **DATE**: reference day to use for the template.

### Reference Map Example

If we consider each user, a **reference map** would provide could provide a reference day for when each user started their subscription.

## Conclusion

In conclusion, Jinja Templates for Recursive Transformations provide a powerful and flexible way to handle complex data transformations and aggregations. By understanding and effectively using these templates, you can significantly improve the efficiency and accuracy of your ETL tasks.

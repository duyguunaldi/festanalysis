WITH partitioned AS
(
         SELECT   order_id,
                  customer_id,
                  event_onsale_date,
                  order_date,
                  ticket_type,
                  ticket_level,
                  payment_type,
                  customer_age_band,
                  ticket_qty,
                  ticket_value,
                  customer_previous_purchase_flag,
                  customer_previous_purchase_festival_flag,
                  customer_previous_purchase_music_flag,
                  customer_previous_purchase_theatre_flag,
                  customer_previous_purchase_sport_flag,
                  Row_number() OVER(partition BY order_id, customer_id ORDER BY order_date) AS seq
         FROM     `tmint-ci-interview.sample_dataset.sample_data`)
SELECT   *
FROM     partitioned
WHERE    seq = 1
ORDER BY order_id
#!/bin/bash
# Q5 (d): writes the design notes with the real numbers of YOUR run
D=~/ca2/data/q5
ORD_ROWS=$(( $(wc -l < $D/orders.csv) - 1 ))
TXN_ROWS=$(( $(wc -l < $D/gateway_txns.csv) - 1 ))
SET_ROWS=$(( $(wc -l < $D/bank_settlements.csv) - 1 ))
FX_ROWS=$(( $(wc -l < $D/fx_rates.csv) - 1 ))
ORD_KB=$(( $(stat -c %s $D/orders.csv) / 1024 ))
TXN_KB=$(( $(stat -c %s $D/gateway_txns.csv) / 1024 ))
FX_KB=$(( $(stat -c %s $D/fx_rates.csv) / 1024 ))
cat > q5_design_notes.txt <<EOF
Q5  DESIGN NOTES
==============================================================================================================
1. WHY TWO CHAINED REDUCE-SIDE JOINS, AND WHY THIS ORDER
   The three tables meet through two DIFFERENT keys:   settlements --(txn_id)--> transactions --(order_id)--> orders
   Job 1 joins on txn_id    (transactions = ONE, settlements = MANY) and writes ONE line per transaction
                            (the settled amount and the number of settlements are already added up).
   Job 2 joins on order_id  (orders = ONE, transaction lines = MANY) and classifies every order.
   Why settlements first:
     * a settlement only knows its txn_id, so it can only be attached to a transaction, never directly to an order.
     * after job 1 the ${SET_ROWS} settlement rows have disappeared into ${TXN_ROWS} transaction lines, so job 2 shuffles
       ${ORD_ROWS} order rows + ${TXN_ROWS} transaction rows and does not carry the settlements again.
     * a duplicated settlement (a transaction settled twice) is reduced to one number in job 1, in constant memory.
     * the opposite order (orders x transactions first, then settlements on txn_id, then re-key by order_id again)
       would need THREE shuffles of the big tables; two joins is the minimum for a chain with two different keys.

2. TAGGED COMPOSITE KEY + SECONDARY SORT  (class Tagged.Key = join id, tag, sub)
   * tag 0 = the "one" record (transaction in job 1, order in job 2) sorts BEFORE tag 1 = the "many" records.
   * The Partitioner and the GroupingComparator use the join id only; the SortComparator uses (id, tag, sub).
   * So every reduce() call receives: the one-record first, then a STREAM of the many-records.
   * The reducers keep only running totals (a sum, a count, some flags, one BigDecimal). They never put the many side in a list.
     The counters MAX_SETTLEMENTS_FOR_ONE_TXN and MAX_TXNS_FOR_ONE_ORDER show how long the streams were.
   * Because the order arrives first, its date, currency and the FX rate of the ORDER DATE are known BEFORE the first transaction is converted.
   * A group without a tag-0 record is an orphan: ORPHAN_SETTLEMENT (job 1) or ORPHAN_TXN (job 2).
     This is the "outer" part of the full outer join.

3. WHY A REPLICATED (MAP-SIDE) JOIN IS NOT SUITABLE FOR ORDERS
   * A replicated join loads the small table completely into the memory of EVERY mapper through the Distributed Cache.
   * fx_rates.csv has ${FX_ROWS} rows (${FX_KB} KB): tiny, so FX is replicated (Distributed Cache, loaded in setup()).
   * orders.csv has ${ORD_ROWS} rows (${ORD_KB} KB here). It is a fact table that grows every day. As Java objects in a HashMap it needs several
     times the file size in EVERY map/reduce task, and it must be copied to every node. It may still fit today, but the design breaks
     as soon as it does not. It also cannot find the ORPHAN_TXN and UNPAID cases, which need the "many" side grouped per order.
   * The other side (transactions, ${TXN_KB} KB) is not small either, so neither input can be broadcast: a reduce-side join is the scalable choice.

4. CLASSIFICATION RULES (class Reconcile), applied in the order given, tolerance INR 1.00 = 100 paise
   UNPAID -> UNDERPAID / OVERPAID -> PAID_NOT_SETTLED -> SETTLEMENT_MISMATCH -> MATCHED
   net paid = sum(SUCCESS) - sum(REFUNDED), each converted at the FX rate of the order date.
   Money is kept as whole paise and the BigDecimal total is rounded only once at the end, so there are no floating-point errors.

5. NUMBERS FROM YOUR RUN
EOF
for c in MATCHED UNDERPAID OVERPAID UNPAID PAID_NOT_SETTLED SETTLEMENT_MISMATCH ORPHAN_TXN; do
  printf "   %-22s %8d rows\n" $c $(cat ~/BigData_CA2/Question5/5b/output/$c/part-r-* | wc -l) >> q5_design_notes.txt
done
printf "   %-22s %8d rows\n" ORPHAN_SETTLEMENT $(cat ~/BigData_CA2/Question5/5a/output/ORPHAN_SETTLEMENT/part-r-* | wc -l) >> q5_design_notes.txt
cat q5_design_notes.txt

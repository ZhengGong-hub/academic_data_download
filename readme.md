step 0: unvierse selection
step 1: download data (download it to a folder named data)
    we download fundamentals data (comp)
    we download price data (crsp)
    we download reference table (somewhere)
    we EVENTUALLY download TAQ 
step 2: calculation (and save the calculation results)
    we calculate factors (p-e, btm etc)
    we calculate CAR 
step 3: join everything we need to form a final.csv



Q: chunks in the interim?

Q: what should we do when we have 2 permco -> 1 gvkey? at a given date 
A: no. never happen after we control for link-table's valid linkdt and linkenddt


note:

book equity -> fund_df['be'] = fund_df['seqq'] + fund_df['txditcq'] - fund_df['pstkq']

note:
for i/s, c/f term, we do rolling sum (pay attention to whether we want to fillna or not)
for b/s term, we use either the t, or the mean of t and t-1 (pay attention to whether we want to fillna or not)
for b/s term, we can fill forward 
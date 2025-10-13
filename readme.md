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


OPTIMISM

we want measure optimism of price target and see whether the retail traders react more to the raw price target or more to the de-biased (de-optimistic) price target.

How to measure optimism of price target?

1. one way is to use ML to forecast the next year's return of a name, and then the difference between pt implied return and the acutal one-year return is the optimism of price target. 
2. to use ML to forecast the error of pt implied return and the actual one-year return, this error is the optimism of price target.
3. also, we could define optimism of a pt as whether this pt is higher than the average/median pt of the same company so far.
3a. against previous consensus pt 
3b. against previous self-posted pt 
4. also, we could define optimism of a pt as how far the pt deviates from the analyst himself's perception of the company's future 1y earnings. measured by pt/1y_eps
4a. we could also compare this self-implied forward PE against previous forward PE of the same analyst. OR against the consensus: consenus of pt/consensus of 1y_eps
5. compare price target revisions with eps revisions. the direction, one goes up one goes down, e.g.. or the magnitude of the change of the two.


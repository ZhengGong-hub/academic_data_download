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
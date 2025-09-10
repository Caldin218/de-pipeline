# System Design (high level)

```mermaid
flowchart LR
  subgraph RAW[Raw]
    E[events_*.ndjson]
    O[orders_*.csv]
    U[users.csv]
  end

  subgraph BRONZE[Bronze]
    BE[events.csv]
    BO[orders.csv]
  end

  subgraph SILVER[Silver]
    DO[daily_orders]
    DR[daily_revenue]
    DAU[daily_active_users]
    CH1[dau_by_channel]
    CH2[revenue_by_channel]
    CONV[daily_conversion]
  end

  subgraph WH[Warehouse (sqlite)]
    T1[(fact_daily_orders)]
    T2[(fact_daily_revenue)]
    T3[(fact_daily_active_users)]
  end

  E --> BE
  O --> BO
  U --> SILVER
  BE --> SILVER
  BO --> SILVER
  SILVER --> WH
```

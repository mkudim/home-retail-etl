CREATE TABLE IF NOT EXISTS public.sales (
    id          BIGSERIAL PRIMARY KEY,
    doc_id      VARCHAR(64)        NOT NULL,
    item        TEXT               NOT NULL,
    category    TEXT               NOT NULL,
    amount      INTEGER            NOT NULL CHECK (amount > 0),
    price       NUMERIC(10, 2)     NOT NULL CHECK (price >= 0),
    discount    NUMERIC(10, 2)     NOT NULL CHECK (discount >= 0),
    shop_num    INTEGER            NOT NULL,
    cash_num    INTEGER            NOT NULL,
    file_name   TEXT               NOT NULL,
    load_dttm   TIMESTAMP          NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_shop_cash
    ON public.sales (shop_num, cash_num);

CREATE INDEX IF NOT EXISTS idx_sales_doc_id
    ON public.sales (doc_id);

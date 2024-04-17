WITH 

-- Mock de dados

customers AS (
    SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' id, 'john.doe@example.com' email, 'John' first_name, 'Doe' last_name, 'M' gender, '123 Main St' address, DATE('1980-01-01') birth_date, '1234567890' phone_number UNION ALL
    SELECT 'b3a3f6e1-4193-4d3d-ba46-1b7b6c472e63' id, 'jane.doe@example.com' email, 'Jane' first_name, 'Doe' last_name, 'F' gender, '456 Maple Ave' address, DATE('1985-02-02') birth_date, '0987654321' phone_number UNION ALL
    SELECT 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' id, 'bob.smith@example.com' email, 'Bob' first_name, 'Smith' last_name, 'M' gender, '789 Oak Dr' address, DATE('1990-03-03') birth_date, '1122334455' phone_number UNION ALL
    SELECT 'd2e5f6f9-6f7d-4ddb-9a00-78c9274d7c2e' id, 'alice.jones@example.com' email, 'Alice' first_name, 'Jones' last_name, 'F' gender, '321 Pine Ln' address, DATE('1995-04-04') birth_date, '5566778899' phone_number UNION ALL
    SELECT 'e5f6g7h8-9i0j-1k2l-3m4n-5o6p7q8r9s0' id, 'david.williams@example.com' email, 'David' first_name, 'Williams' last_name, 'M' gender, '987 Elm St' address, DATE('1988-05-05') birth_date, '5544332211' phone_number UNION ALL
    SELECT 'f6g7h8i9-0j1k-2l3m-4n5o-6p7q8r9s0t' id, 'emily.brown@example.com' email, 'Emily' first_name, 'Brown' last_name, 'F' gender, '654 Oak Ave' address, DATE('1993-06-06') birth_date, '9988776655' phone_number UNION ALL
    SELECT 'g7h8i9j0-1k2l-3m4n-5o6p-7q8r9s0t1u' id, 'michael.johnson@example.com' email, 'Michael' first_name, 'Johnson' last_name, 'M' gender, '321 Maple Ln' address, DATE('1982-07-07') birth_date, '1122334455' phone_number UNION ALL
    SELECT 'h9i0j1k2-3l4m-5n6o-7p8q9r0s1' id, 'sarah.jones@example.com' email, 'Sarah' first_name, 'Jones' last_name, 'F' gender, '789 Elm St' address, DATE('1990-01-01') birth_date, '1122334455' phone_number UNION ALL
    SELECT 'i2j3k4l5-6m7n-8o9p-1q2r-3s4t5u6v7w' id, 'michael.smith@example.com' email, 'Michael' first_name, 'Smith' last_name, 'M' gender, '987 Maple Ave' address, DATE('1990-01-01') birth_date, '9988776655' phone_number UNION ALL
    SELECT 'j3k4l5m6-7n8o-9p1q-2r3s-4t5u6v7w8x' id, 'jessica.brown@example.com' email, 'Jessica' first_name, 'Brown' last_name, 'F' gender, '654 Pine Ln' address, DATE('1990-01-01') birth_date, '5544332211' phone_number
), categories AS (
    SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' id, 'Celulares' name, 'Celulares' path UNION ALL
    SELECT 'b3a3f6e1-4193-4d3d-ba46-1b7b6c472e63' id, 'Eletrônicos' name, 'Eletrônicos' path UNION ALL
    SELECT 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' id, 'Livros' name, 'Livros' path
), items AS (
    SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' id, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' seller_id, 'iPhone 11' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 1000.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'b3a3f6e1-4193-4d3d-ba46-1b7b6c472e63' id, 'b3a3f6e1-4193-4d3d-ba46-1b7b6c472e63' seller_id, 'Samsung Galaxy S20' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 900.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' id, 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' seller_id, 'O Senhor dos Anéis' name, 'Livro de fantasia' description, 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' category_id, 50.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'd2e5f6f9-6f7d-4ddb-9a00-78c9274d7c2e' id, 'd2e5f6f9-6f7d-4ddb-9a00-78c9274d7c2e' seller_id, 'Motorola Moto G8' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 800.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'e5f6g7h8-9i0j-1k2l-3m4n-5o6p7q8r9s0' id, 'e5f6g7h8-9i0j-1k2l-3m4n-5o6p7q8r9s0' seller_id, 'Xiaomi Redmi Note 9' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 700.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'f6g7h8i9-0j1k-2l3m-4n5o-6p7q8r9s0t' id, 'f6g7h8i9-0j1k-2l3m-4n5o-6p7q8r9s0t' seller_id, 'PlayStation 5' name, 'Console de videogame' description, 'b3a3f6e1-4193-4d3d-ba46-1b7b6c472e63' category_id, 2500.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'g7h8i9j0-1k2l-3m4n-5o6p-7q8r9s0t1u' id, 'g7h8i9j0-1k2l-3m4n-5o6p-7q8r9s0t1u' seller_id, 'Nintendo Switch' name, 'Console de videogame' description, 'b3a3f6e1-4193-4d3d-ba46-1b7b6c472e63' category_id, 1500.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'h9i0j1k2-3l4m-5n6o-7p8q9r0s1' id, 'h9i0j1k2-3l4m-5n6o-7p8q9r0s1' seller_id, 'Harry Potter e a Pedra Filosofal' name, 'Livro de fantasia' description, 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' category_id, 30.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'i2j3k4l5-6m7n-8o9p-1q2r-3s4t5u6v7w' id, 'i2j3k4l5-6m7n-8o9p-1q2r-3s4t5u6v7w' seller_id, 'As Crônicas de Nárnia' name, 'Livro de fantasia' description, 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' category_id, 40.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'j3k4l5m6-7n8o-9p1q-2r3s-4t5u6v7w8x' id, 'j3k4l5m6-7n8o-9p1q-2r3s-4t5u6v7w8x' seller_id, 'Kindle Paperwhite' name, 'Leitor de livros digitais' description, 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' category_id, 120.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'k4l5m6n7-8o9p-1q2r-3s4t-5u6v7w8x9y' id, 'g7h8i9j0-1k2l-3m4n-5o6p-7q8r9s0t1u' seller_id, 'Motorola Moto G9' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 900.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'l5m6n7o8-9p1q-2r3s-4t5u-6v7w8x9y0z' id, 'd2e5f6f9-6f7d-4ddb-9a00-78c9274d7c2e' seller_id, 'Samsung Galaxy A51' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 800.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'm6n7o8p9-1q2r-3s4t-5u6v-7w8x9y0z1a' id, 'b3a3f6e1-4193-4d3d-ba46-1b7b6c472e63' seller_id, 'Xiaomi Redmi Note 10' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 700.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'n7o8p9q1-2r3s-4t5u-6v7w-8x9y0z1a2b' id, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' seller_id, 'iPhone SE' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 600.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'o8p9q1r2-3s4t-5u6v-7w8x-9y0z1a2b3c' id, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' seller_id, 'Motorola Moto G10' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 500.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'p9q1r2s3-4t5u-6v7w-8x9y-0z1a2b3c4d' id, 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' seller_id, 'Samsung Galaxy A71' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 400.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'q1r2s3t4-5u6v-7w8x-9y0z-1a2b3c4d5e' id, 'g7h8i9j0-1k2l-3m4n-5o6p-7q8r9s0t1u' seller_id, 'Xiaomi Redmi Note 8' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 300.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 'r2s3t4u5-6v7w-8x9y-0z1a-2b3c4d5e6f' id, 'f6g7h8i9-0j1k-2l3m-4n5o-6p7q8r9s0t' seller_id, 'iPhone XR' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 200.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 's3t4u5v6-7w8x-9y0z-1a2b-3c4d5e6f7g' id, 'f6g7h8i9-0j1k-2l3m-4n5o-6p7q8r9s0t' seller_id, 'Motorola Moto G7' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 100.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    SELECT 't4u5v6w7-8x9y-0z1a-2b3c-4d5e6f7g8h' id, 'c1d4f6f8-5f6d-4ddb-8a00-67b9273d6c1e' seller_id, 'Samsung Galaxy A21s' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 50.00 price, 'ACTIVE' status, NULL cancel_date
), orders_factory AS (
    SELECT c.id customer_id, 
        i.id item_id,
        i.price,
        m.i month_number, 
        c.birth_date, 
        ROUND(RAND() * 160) order_size
    FROM customers c
    INNER JOIN items i on c.id <> i.seller_id
    INNER JOIN (
        SELECT '1' AS i
        UNION ALL SELECT '2' AS i
        UNION ALL SELECT '3' AS i
        UNION ALL SELECT '4' AS i
        UNION ALL SELECT '5' AS i
        UNION ALL SELECT '6' AS i
    ) m ON true
), orders AS (
    SELECT uuid() id, 
        of.customer_id, 
        of.item_id, 
        date_add(
            'hour', 
            CAST(ROUND(24*30*rand()) AS BIGINT), 
            CAST('2020-0' || of.month_number || '-01 00:00:00' AS TIMESTAMP)
        ) order_date,
        of.price * (0.95+0.1*RAND()) price, -- gerando um preço entre 95% e 105% do preço original
        RAND(1, 10) quantity
    FROM orders_factory of
    CROSS JOIN UNNEST (SEQUENCE(1, CAST(of.order_size AS BIGINT))) order_size(i)
), 

item_cdc AS (
    -- dia 1
    SELECT 
        'asdfasdf-9c0b-4ef8-bb6d-6bb9bd380a11' update_id, TIMESTAMP '2023-01-01 01:15:20.123' update_date, 'INSERT' update_type, -- metadata
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' id, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' seller_id, 'iPhone 11' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 1000.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    -- dia 2
    SELECT 
        'asdfasdf-9c0b-4ef8-bb6d-6bb9bd380a11' update_id, TIMESTAMP '2023-01-02 01:15:20.123' update_date, 'UPDATE' update_type, -- metadata
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' id, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' seller_id, 'iPhone 11' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 2000.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    -- dia 3
    SELECT 
        'asdfasdf-9c0b-4ef8-bb6d-6bb9bd380a11' update_id, TIMESTAMP '2023-01-03 01:15:20.123' update_date, 'UPDATE' update_type, -- metadata
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' id, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' seller_id, 'iPhone 11' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 3000.00 price, 'ACTIVE' status, NULL cancel_date UNION ALL
    -- dia 3 um pouco mais tarde
    SELECT 
        'asdfasdf-9c0b-4ef8-bb6d-6bb9bd380a11' update_id, TIMESTAMP '2023-01-03 02:15:20.123' update_date, 'UPDATE' update_type, -- metadata
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' id, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' seller_id, 'iPhone 11' name, 'Aparelho de celular' description, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' category_id, 1000.00 price, 'CANCELED' status, NULL cancel_date
),



-- Liste usuários com aniversário de hoje cujo número de vendas realizadas em janeiro de 2020 seja superior a 1500.

solution_1 AS (
    SELECT c.id, c.birth_date,
        DATE_FORMAT(c.birth_date, '%m-%d') = DATE_FORMAT(DATE('2023-01-01'), '%m-%d') is_birthday, -- use current_date for the real case
        COUNT(o.id) > 1500 is_order_count_greater_than_1500,
        COUNT(o.id) order_count,
        COUNT(o.id) > 1500 AND DATE_FORMAT(c.birth_date, '%m-%d') = DATE_FORMAT(DATE('2023-01-01'), '%m-%d') should_be_included
    FROM customers c
    INNER JOIN items i
        ON c.id = i.seller_id
        -- AND DATE_FORMAT(c.birth_date, '%m-%d') = DATE_FORMAT(DATE('2023-01-01'), '%m-%d')
    INNER JOIN orders o
        ON c.id = o.customer_id
        AND DATE_FORMAT(o.order_date, '%Y-%m') = '2020-01'
    GROUP BY 1, 2, 3
    -- HAVING COUNT(o.id) > 1500
), solution_2_revenue AS (
    SELECT DATE_FORMAT(o.order_date, '%Y-%m') order_month,
        c.id,
        c.first_name, c.last_name,
        SUM(o.price * o.quantity) total_revenue,
        SUM(o.quantity) total_quantity,
        AVG(o.price * o.quantity) avg_ticket,
        COUNT(o.id) order_count
    FROM orders o
    INNER JOIN items i
        ON o.item_id = i.id
        AND DATE_FORMAT(o.order_date, '%Y') = '2020'
    INNER JOIN categories cat
        ON i.category_id = cat.id
        AND cat.name = 'Celulares'
    INNER JOIN customers c
        ON o.customer_id = c.id
    GROUP BY 1, 2, 3, 4
), 


-- Para cada mês de 2020, são solicitados os 5 principais usuários que mais venderam (R$) na categoria Celulares. 
-- São obrigatórios o mês e ano da análise, nome e sobrenome do vendedor, quantidade de vendas realizadas, quantidade de produtos vendidos e valor total transacionado.

solution_2 AS (
    SELECT order_month,
        first_name, last_name,
        total_revenue,
        total_quantity,
        order_count,
        avg_ticket,
        RANK() OVER (PARTITION BY order_month ORDER BY total_revenue DESC) revenue_rank,
        RANK() OVER (PARTITION BY order_month ORDER BY total_revenue DESC) <= 5 should_be_included
    FROM solution_2_revenue
),


-- É solicitada uma nova tabela a ser preenchida com o preço e status dos Itens no final do dia.
-- Lembre-se de que deve ser reprocessável.
-- Vale ressaltar que na tabela Item teremos apenas o último status informado pelo PK definido.
-- (Pode ser resolvido através de StoredProcedure)

solution_3 AS (
    SELECT calendar.dt,
        item_cdc.id,
        MAX_BY(item_cdc.status, item_cdc.update_date) status,
        MAX_BY(item_cdc.price, item_cdc.update_date) price
    FROM item_cdc
    INNER JOIN (
        SELECT dt.dt
        FROM UNNEST(sequence(TIMESTAMP '2023-01-01 00:00:00', TIMESTAMP '2023-01-05 00:00:00', interval '1' day)) dt(dt)
    ) calendar
    ON
        item_cdc.update_date <= calendar.dt
    GROUP BY 1, 2
    ORDER BY 2, 1

)

SELECT *
FROM solution_3
-- ORDER BY order_month, revenue_rank
-- ORDER BY is_birthday DESC, order_count DESC

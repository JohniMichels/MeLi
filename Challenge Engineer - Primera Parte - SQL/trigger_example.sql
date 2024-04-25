-- usando eventsourcing para fazer um cdc de tabela

CREATE TABLE Items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seller_id UUID NOT NULL,
    name VARCHAR(60) NOT NULL, -- Procurei e parece que o limite no Mercado Livre é de 60 caracteres
    description TEXT, -- usando TEXT para permitir descrições mais longas, vi que o limite no Mercado Livre é de 10000 caracteres que é maior que o limite do tipo VARCHAR
    category_id UUID NOT NULL,
    price DECIMAL(10, 2) NOT NULL, -- Usando DECIMAL para representar valores monetários, limite de 10 dígitos sendo 2 deles decimais. Isso significa que o valor máximo que podemos representar é 100 milhões
    status VARCHAR(10) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'CANCELED')),
    cancel_date TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (category_id) REFERENCES Categories(id),
    FOREIGN KEY (seller_id) REFERENCES Customers(id)
);

CREATE TABLE ItemCDC (
    update_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    update_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_type VARCHAR(10) NOT NULL CHECK (update_type IN ('INSERT', 'UPDATE', 'DELETE')),
    id UUID NOT NULL,
    seller_id UUID NOT NULL,
    name VARCHAR(60) NOT NULL, -- Procurei e parece que o limite no Mercado Livre é de 60 caracteres
    description TEXT, -- usando TEXT para permitir descrições mais longas, vi que o limite no Mercado Livre é de 10000 caracteres que é maior que o limite do tipo VARCHAR
    category_id UUID NOT NULL,
    price DECIMAL(10, 2) NOT NULL, -- Usando DECIMAL para representar valores monetários, limite de 10 dígitos sendo 2 deles decimais. Isso significa que o valor máximo que podemos representar é 100 milhões
    status VARCHAR(10) NOT NULL CHECK (status IN ('ACTIVE', 'INACTIVE', 'CANCELED')),
    cancel_date TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES Categories(id),
    FOREIGN KEY (seller_id) REFERENCES Customers(id),
    FOREIGN KEY (id) REFERENCES Items(id)
);

CREATE OR REPLACE FUNCTION NotifyItemChanged()
RETURNS TRIGGER AS $itemcdcnotification$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO itemcdc (
            update_type, id, seller_id, name, description, category_id, price, status, cancel_date
        ) 
        SELECT TG_OP, OLD.*;
    ELSE
        INSERT INTO itemcdc (
            update_type, id, seller_id, name, description, category_id, price, status, cancel_date
        )
        SELECT TG_OP, NEW.*;
    END IF;
    RETURN NULL;
END;
$itemcdcnotification$ LANGUAGE plpgsql;

CREATE TRIGGER ItemCDCTrigger
AFTER INSERT OR UPDATE OR DELETE
ON Items
FOR EACH ROW
EXECUTE FUNCTION NotifyItemChanged();

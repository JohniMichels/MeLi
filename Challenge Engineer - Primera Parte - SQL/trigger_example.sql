-- usando eventsourcing para fazer um cdc de tabela

CREATE TABLE Item (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    seller_id UUID NOT NULL,
    name VARCHAR(60) NOT NULL, -- Procurei e parece que o limite no Mercado Livre é de 60 caracteres
    description TEXT, -- usando TEXT para permitir descrições mais longas, vi que o limite no Mercado Livre é de 10000 caracteres que é maior que o limite do tipo VARCHAR
    category_id UUID NOT NULL,
    price DECIMAL(10, 2) NOT NULL, -- Usando DECIMAL para representar valores monetários, limite de 10 dígitos sendo 2 deles decimais. Isso significa que o valor máximo que podemos representar é 100 milhões
    status VARCHAR(10) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'INACTIVE', 'CANCELED')),
    cancel_date TIMESTAMP DEFAULT NULL,
    FOREIGN KEY (category_id) REFERENCES Category(id),
    FOREIGN KEY (seller_id) REFERENCES Customer(id)
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
    FOREIGN KEY (seller_id) REFERENCES Categories(id),
    FOREIGN KEY (id) REFERENCES Items(id)
);

CREATE FUNCTION NotifyItemChanged()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') OR (TG_OP = 'UPDATE') THEN
        INSERT INTO ItemCDC (update_type, id, seller_id, name, description, category_id, price, status, cancel_date)
        VALUES (TG_OP, NEW.id, NEW.seller_id, NEW.name, NEW.description, NEW.category_id, NEW.price, NEW.status, NEW.cancel_date);
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO ItemCDC (update_type, id, seller_id, name, description, category_id, price, status, cancel_date)
        VALUES ('DELETE', OLD.id, OLD.seller_id, OLD.name, OLD.description, OLD.category_id, OLD.price, OLD.status, OLD.cancel_date);
    END IF;
    RETURN NEW;
END;

CREATE TRIGGER ItemCDCTrigger
AFTER INSERT OR UPDATE OR DELETE
ON Items
FOR EACH ROW
EXECUTE FUNCTION NotifyItemChanged();

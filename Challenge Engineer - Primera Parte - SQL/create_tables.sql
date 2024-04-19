
-- Customer: É a entidade onde se encontram todos os nossos clientes, sejam eles Compradores ou Vendedores do Site.
-- Os principais atributos são email, nome, sobrenome, sexo, endereço, data de nascimento, telefone, entre outros.
CREATE TABLE Customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(200) NOT NULL, -- Matéria abaixo indica o porque desse limite
    last_name VARCHAR(200) NOT NULL, -- http://atl.clicrbs.com.br/mundobom/2011/01/20/nome-proprio-mais-comprido-do-mundo-tem-29-palavras-e-197-letras/
    gender CHAR(1),
    address TEXT,
    birth_date DATE,
    phone VARCHAR(15) -- Assumindo somente números e respeitando o tamanho máximo definido no E.164
);

-- Category: É a entidade onde se encontra a descrição de cada categoria com seu respectivo caminho. Cada item possui uma categoria associada a ele.
CREATE TABLE Categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(60) NOT NULL UNIQUE, --
    path TEXT NOT NULL
);

-- Item: É a entidade onde estão localizados os produtos publicados em nosso marketplace. 
-- O volume é muito grande porque estão incluídos todos os produtos que foram publicados em algum momento. 
-- Usando o status do item ou a data de cancelamento, você pode detectar os itens ativos no marketplace. 
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


CREATE INDEX idx_item_status ON Items(status); 
CREATE INDEX idx_item_cancel_date ON Items(cancel_date);

-- Order: O pedido é a entidade que reflete as transações geradas dentro do site (cada compra é um pedido).
-- Neste caso não teremos fluxo de carrinho de compras, portanto cada item vendido será refletido em um pedido independente da quantidade que foi adquirida.
CREATE TABLE Orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL,
    item_id UUID NOT NULL,
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    price DECIMAL(10, 2) NOT NULL, -- Como o preço do item muda de acordo com uma das perguntas, é necessário armazenar o preço práticado no momento da compra
    quantity INT NOT NULL, -- Um dos pontos sugerem que mesmo sem carrinho a compra pode ter mais de uma unidade do mesmo item
    FOREIGN KEY (customer_id) REFERENCES Customers(id),
    FOREIGN KEY (item_id) REFERENCES Items(id)
);

-- Para todos escolhi usar UUID como chave primária,
-- pois, por mais que seja um pouco mais custoso para o banco de dados,
-- é mais seguro e escalável, além de ser mais difícil de ser adivinhado.

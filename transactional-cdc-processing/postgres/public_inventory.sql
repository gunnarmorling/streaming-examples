SET search_path TO inventory;

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 10001),
  order_date DATE NOT NULL,
  purchaser INT NOT NULL REFERENCES customers(id),
  shipping_address TEXT NOT NULL
);
ALTER TABLE orders REPLICA IDENTITY FULL;

CREATE TABLE order_lines (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 100001),
  order_id INT NOT NULL REFERENCES orders(id),
  product_id INT NOT NULL REFERENCES products(id),
  quantity INT NOT NULL,
  price NUMERIC(10, 2) NOT NULL
);
ALTER TABLE order_lines REPLICA IDENTITY FULL;

/*

SET search_path TO inventory;
  INSERT INTO orders (order_date, purchaser, shipping_address)
  VALUES (CURRENT_DATE, 1001, '123 Main Street, Springfield, IL 62701');

  INSERT INTO order_lines (order_id, product_id, quantity, price)
  VALUES
    (10001, 101, 2, 19.99),
    (10001, 102, 1, 49.99);
    
    
  INSERT INTO orders (order_date, purchaser, shipping_address)
  VALUES (CURRENT_DATE, 1002, '456 Oak Avenue, Chicago, IL 60601');

  INSERT INTO order_lines (order_id, product_id, quantity, price)
  VALUES
    (10002, 101, 1, 19.99),
    (10002, 102, 3, 49.99),
    (10002, 103, 2, 29.99);
    */
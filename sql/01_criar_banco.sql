-- Rodar UMA vez como postgres, no primário (hoje pg2). A senha é pedida pelo \password (não fica no histórico nem no log).
CREATE ROLE cyberfinance_app LOGIN;
\password cyberfinance_app
CREATE DATABASE cyberfinance OWNER cyberfinance_app ENCODING 'UTF8';
-- ninguém além do dono conecta no banco novo
REVOKE CONNECT ON DATABASE cyberfinance FROM PUBLIC;
\l cyberfinance

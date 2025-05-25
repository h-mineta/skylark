# Skylark
Skylark - 雲雀(競馬予測素体作成)


```sql
CREATE DATABASE skylark CHARACTER SET utf8mb4;
CREATE USER 'skylark'@'%' IDENTIFIED BY 'skylarkpw!';
GRANT ALL PRIVILEGES ON skylark . * TO 'skylark'@'%';
FLUSH PRIVILEGES;
```

```bash
sudo dnf install -y chromium-headless at-spi2-atk
pip3.12 install -U -r requirements.txt
playwright install chromium-headless-shell
./app.py -U -S -F
streamlit run webui.py
```

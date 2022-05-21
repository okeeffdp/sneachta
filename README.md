Sneachta
========

> Sneacht - The Irish word for snow

## Installation
```bash
git clone ...

cd ./sneachta

pip install -e .
```
## Usage
Basic setup
```python
from sneachta import SnowflakeClient

client = SnowflakeClient(
    ...
)
```

Download data
```python
df = client.query("SELECT 1")
```

Create a table
```python
df = pd.DataFrame([1, "a"], columns=["c1", "c2"])
client.create_from_dataframe(df, "my_table")
```
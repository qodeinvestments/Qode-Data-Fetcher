from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

model_name = "defog/sqlcoder-7b-2"

tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained(model_name, device_map="auto", torch_dtype=torch.float16)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)

def nl_to_sql(prompt: str) -> str:
    system_prompt = """### Task
Generate a SQL query for DuckDB that answers the following question.

### Database Schema
The database has tables such as:
- options_nifty_20240101_18000_C
- index_nifty50
- futures_banknifty_20240101

Each has columns:
- timestamp, o, h, l, c, v, oi

### Question
""" + prompt + "\n\n### SQL Query\n"

    inputs = tokenizer(system_prompt, return_tensors="pt").to(device)
    
    print("Generating SQL query for:", prompt)
    print("System prompt:", system_prompt)
    print("Inputs:", inputs)

    outputs = model.generate(
        **inputs,
        max_new_tokens=256,
        temperature=0.2,
        top_p=0.95,
        do_sample=False,
        pad_token_id=tokenizer.eos_token_id,
    )
    
    print("Generated outputs:", outputs)

    decoded = tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    print("Decoded output:", decoded)
    
    if "```sql" in decoded:
        decoded = decoded.split("```sql")[1].split("```")[0].strip()
    elif "SELECT" in decoded:
        decoded = decoded.split("SELECT", 1)[1]
        decoded = "SELECT " + decoded.split("###")[0].strip()

    return decoded.strip()

nl_to_sql("What is the average closing price of Nifty options on 2024-01-01?")
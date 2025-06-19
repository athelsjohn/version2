import React, { useState } from "react";

function OrderForm() {
  const [form, setForm] = useState({
    Order_ID: "",
    Customer_ID: "",
    Warehouse_ID: "",
    Customer_Age: "",
    Customer_Gender: "",
    Date: "",
    Product_ID: "",
    SKU_ID: "",
    Category: "",
    Quantity: "",
    Price_per_Unit: "",
  });
  const [result, setResult] = useState(null);

  const handleChange = (e) => {
    setForm({ ...form, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setResult(null);
    try {
      const response = await fetch("/orders", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      const data = await response.json();
      setResult(data.message || JSON.stringify(data));
    } catch (err) {
      setResult("Error: " + err.message);
    }
  };

  return (
    <section>
      <h2>Add Order</h2>
      <form onSubmit={handleSubmit} style={{ display: "grid", gap: 8 }}>
        {Object.keys(form).map((key) => (
          <input
            key={key}
            name={key}
            value={form[key]}
            onChange={handleChange}
            placeholder={key.replace(/_/g, " ")}
            required
          />
        ))}
        <button type="submit">Submit Order</button>
      </form>
      {result && <div style={{ marginTop: 8 }}>{result}</div>}
    </section>
  );
}

export default OrderForm;

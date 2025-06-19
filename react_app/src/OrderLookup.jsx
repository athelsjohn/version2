// OrderLookup.jsx
import React, { useState } from "react";

function OrderLookup() {
  const [query, setQuery] = useState({
    order_id: "",
    product_id: "",
    sku_id: "",
  });
  const [exists, setExists] = useState(null);

  const handleChange = (e) => {
    setQuery({ ...query, [e.target.name]: e.target.value });
  };

  const handleCheck = async (e) => {
    e.preventDefault();
    setExists(null);
    const params = new URLSearchParams(query).toString();
    try {
      const response = await fetch(`/orders?${params}`);
      const data = await response.json();
      setExists(data.exists ? "Order exists." : "Order does not exist.");
    } catch (err) {
      setExists("Error: " + err.message);
    }
  };

  return (
    <section>
      <h2>Check Order Existence</h2>
      <form onSubmit={handleCheck} style={{ display: "flex", gap: 8 }}>
        <input
          name="order_id"
          value={query.order_id}
          onChange={handleChange}
          placeholder="Order ID"
          required
        />
        <input
          name="product_id"
          value={query.product_id}
          onChange={handleChange}
          placeholder="Product ID"
          required
        />
        <input
          name="sku_id"
          value={query.sku_id}
          onChange={handleChange}
          placeholder="SKU ID"
          required
        />
        <button type="submit">Check</button>
      </form>
      {exists && <div style={{ marginTop: 8 }}>{exists}</div>}
    </section>
  );
}

export default OrderLookup;

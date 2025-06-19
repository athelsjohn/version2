import React, { useState } from "react";

function UserRecommendation() {
  const [customerId, setCustomerId] = useState("");
  const [recommendations, setRecommendations] = useState(null);
  const [error, setError] = useState(null);

  const handleRecommend = async (e) => {
    e.preventDefault();
    setRecommendations(null);
    setError(null);
    try {
      const response = await fetch(`/users?customer_id=${customerId}`, {
        method: "POST",
      });
      const text = await response.text();
      if (!text) {
        setError("No data returned from server.");
        return;
      }
      let data;
      try {
        data = JSON.parse(text);
      } catch (err) {
        setError("Invalid JSON from server.");
        return;
      }
      if (data.message) {
        setError(data.message);
        return;
      }
      if (data.detail) {
        setError(data.detail);
        return;
      }
      setRecommendations(
        data.recommended_products ||
          data.recommendations ||
          JSON.stringify(data)
      );
    } catch (err) {
      setError("Error: " + err.message);
    }
  };

  return (
    <section>
      <h2>User Recommendations</h2>
      <form onSubmit={handleRecommend} style={{ display: "flex", gap: 8 }}>
        <input
          value={customerId}
          onChange={(e) => setCustomerId(e.target.value)}
          placeholder="Customer ID"
          required
        />
        <button type="submit">Get Recommendations</button>
      </form>
      {recommendations && (
        <div style={{ marginTop: 8 }}>
          <strong>Recommendations:</strong>
          <pre>
            {typeof recommendations === "string"
              ? recommendations
              : JSON.stringify(recommendations, null, 2)}
          </pre>
        </div>
      )}
      {error && <div style={{ color: "red", marginTop: 8 }}>{error}</div>}
    </section>
  );
}

export default UserRecommendation;

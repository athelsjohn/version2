import React, { useState } from "react";
import OrderForm from "./OrderForm";
import OrderLookup from "./OrderLookup";
import UserRecommendation from "./UserRecommendation";

function App() {
  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: 24 }}>
      <h1>Order Processing & Recommendation System</h1>
      <OrderForm />
      <hr />
      <OrderLookup />
      <hr />
      <UserRecommendation />
    </div>
  );
}

export default App;

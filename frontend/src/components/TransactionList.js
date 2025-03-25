import React, { useState, useEffect } from "react";
import "./TransactionList.css";

function TransactionList() {
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    console.log("Fetching transactions...");
    fetch("/api/transactions")
      .then((response) => {
        console.log("Response:", response);
        return response.json();
      })
      .then((data) => {
        console.log("Data received:", data);
        setTransactions(data);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Error:", err);
        setError(err.message);
        setLoading(false);
      });
  }, []);

  if (loading) return <div>Loading transactions...</div>;
  if (error) return <div>Error loading transactions: {error}</div>;
  if (!transactions.length) return <div>No transactions found</div>;

  console.log("Rendering transactions:", transactions);

  return (
    <div className="transaction-list">
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Amount</th>
            <th>Currency</th>
            <th>Customer ID</th>
            <th>Customer Email</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {transactions.map((tx) => (
            <tr key={tx.stripe_id || Math.random()}>
              <td>
                {tx.stripe_created_date_utc
                  ? new Date(tx.stripe_created_date_utc).toLocaleDateString()
                  : "N/A"}
              </td>
              <td>{tx.stripe_converted_amount || "N/A"}</td>
              <td>{tx.stripe_converted_currency || "N/A"}</td>
              <td>{tx.stripe_customer_id || "N/A"}</td>
              <td>{tx.stripe_customer_email || "N/A"}</td>
              <td>
                <span className={`status ${tx.merge_source}`}>
                  {tx.merge_source}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default TransactionList;

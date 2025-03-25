import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import "./Transactions.css";

function Transactions() {
  const navigate = useNavigate();
  const [transactions, setTransactions] = useState([]);
  const [filteredTransactions, setFilteredTransactions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [statusFilter, setStatusFilter] = useState("all");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [tableType, setTableType] = useState("started_matches");

  useEffect(() => {
    console.log("Fetching transactions...");
    setLoading(true);
    fetch(`http://localhost:5001/api/transactions?table=${tableType}`)
      .then((response) => {
        console.log("Response:", response);
        return response.json();
      })
      .then((data) => {
        console.log("Data received:", data);
        setTransactions(data);
        setFilteredTransactions(data);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Error:", err);
        setError(err.message);
        setLoading(false);
      });
  }, [tableType]); // Re-fetch when table type changes

  // Get unique statuses for the dropdown
  const statuses = [
    "all",
    ...new Set(transactions.map((tx) => tx.merge_source)),
  ];

  // Handle filters
  const applyFilters = () => {
    let filtered = [...transactions];

    // Apply status filter
    if (statusFilter !== "all") {
      filtered = filtered.filter((tx) => tx.merge_source === statusFilter);
    }

    // Apply date filters
    if (startDate) {
      filtered = filtered.filter(
        (tx) => new Date(tx.stripe_created_date_utc) >= new Date(startDate)
      );
    }
    if (endDate) {
      filtered = filtered.filter(
        (tx) => new Date(tx.stripe_created_date_utc) <= new Date(endDate)
      );
    }

    setFilteredTransactions(filtered);
  };

  // Update filters when any filter changes
  useEffect(() => {
    applyFilters();
  }, [statusFilter, startDate, endDate]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleTransactionClick = (tx) => {
    navigate(`/transactions/${tx.stripe_id}`, { state: { transaction: tx } });
  };

  if (loading) return <div>Loading transactions...</div>;
  if (error) return <div>Error loading transactions: {error}</div>;
  if (!transactions.length) return <div>No transactions found</div>;

  return (
    <div className="main-content">
      <div className="header">
        <div className="filters">
          <select
            value={tableType}
            onChange={(e) => setTableType(e.target.value)}
            className="table-filter"
          >
            <option value="started_matches">Started Transactions</option>
            <option value="succeeded_matches">Succeeded Transactions</option>
          </select>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="date-filter"
            placeholder="Start Date"
          />
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="date-filter"
            placeholder="End Date"
          />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className="status-filter"
          >
            {statuses.map((status) => (
              <option key={status} value={status}>
                {status === "all" ? "All Statuses" : status}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="transaction-list">
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Amount</th>
              <th>Currency</th>
              <th>Customer ID</th>
              <th>Customer Email</th>
              <th>PaymentIntent ID</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {filteredTransactions.map((tx) => (
              <tr
                key={tx.stripe_id || Math.random()}
                onClick={() => handleTransactionClick(tx)}
                className="clickable-row"
              >
                <td>
                  {tx.stripe_created_date_utc
                    ? new Date(tx.stripe_created_date_utc).toLocaleDateString()
                    : "N/A"}
                </td>
                <td>{tx.stripe_converted_amount || "N/A"}</td>
                <td>{tx.stripe_converted_currency || "N/A"}</td>
                <td>{tx.stripe_customer_id || "N/A"}</td>
                <td>{tx.stripe_customer_email || "N/A"}</td>
                <td>{tx.stripe_paymentintent_id || "N/A"}</td>
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
    </div>
  );
}

export default Transactions;

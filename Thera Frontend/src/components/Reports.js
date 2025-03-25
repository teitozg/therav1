import React, { useState, useEffect } from "react";
import "./Reports.css";

function Reports() {
  const [transactions, setTransactions] = useState([]);
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
      .then((response) => response.json())
      .then((data) => {
        setTransactions(data);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Error:", err);
        setError(err.message);
        setLoading(false);
      });
  }, [tableType]);

  const statuses = [
    "all",
    ...new Set(transactions.map((tx) => tx.merge_source)),
  ];

  const handleDownload = () => {
    let filteredData = [...transactions];

    if (statusFilter !== "all") {
      filteredData = filteredData.filter(
        (tx) => tx.merge_source === statusFilter
      );
    }

    if (startDate) {
      filteredData = filteredData.filter(
        (tx) => new Date(tx.stripe_created_date_utc) >= new Date(startDate)
      );
    }

    if (endDate) {
      filteredData = filteredData.filter(
        (tx) => new Date(tx.stripe_created_date_utc) <= new Date(endDate)
      );
    }

    const headers = [
      "Date",
      "Amount",
      "Currency",
      "Customer ID",
      "Customer Email",
      "PaymentIntent ID",
      "Mode",
      "Payment Source Type",
      "Card Brand",
      "Fee",
      "Seller Message",
      "Status",
    ];

    const csvData = [
      headers.join(","),
      ...filteredData.map((tx) =>
        [
          new Date(tx.stripe_created_date_utc).toLocaleDateString(),
          tx.stripe_converted_amount || "",
          tx.stripe_converted_currency || "",
          tx.stripe_customer_id || "",
          tx.stripe_customer_email || "",
          tx.stripe_paymentintent_id || "",
          tx.stripe_mode || "",
          tx.stripe_payment_source_type || "",
          tx.stripe_card_brand || "",
          tx.stripe_fee || "",
          tx.stripe_seller_message || "",
          tx.merge_source || "",
        ].join(",")
      ),
    ].join("\n");

    const blob = new Blob([csvData], { type: "text/csv" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${tableType}_${new Date().toISOString().split("T")[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
  };

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div className="reports-container">
      <div className="reports-content">
        <h2>Download Reports</h2>
        <p className="description">
          Select your filters and download transaction reports
        </p>
        <div className="filters-panel">
          <div className="filter-group">
            <label>Transaction Type</label>
            <select
              value={tableType}
              onChange={(e) => setTableType(e.target.value)}
              className="table-filter"
            >
              <option value="started_matches">Started Transactions</option>
              <option value="succeeded_matches">Succeeded Transactions</option>
            </select>
          </div>

          <div className="filter-group">
            <label>Date Range</label>
            <div className="date-inputs">
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
            </div>
          </div>

          <div className="filter-group">
            <label>Status</label>
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

          <button onClick={handleDownload} className="download-btn">
            Download CSV
          </button>
        </div>
      </div>
    </div>
  );
}

export default Reports;

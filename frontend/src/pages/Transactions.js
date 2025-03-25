import React from "react";
import TransactionList from "../components/TransactionList";

function Transactions() {
  return (
    <div className="main-content">
      <div className="header">
        <div className="filters">
          <button>All sources</button>
          <button>All time</button>
        </div>
        <button className="export-btn">Export</button>
      </div>
      <TransactionList />
    </div>
  );
}

export default Transactions;

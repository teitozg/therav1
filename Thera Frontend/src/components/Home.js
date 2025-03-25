import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import {
  MdDashboard,
  MdPayments,
  MdAssessment,
  MdCloudUpload,
} from "react-icons/md";
import "./Home.css";

function Home() {
  const [metrics, setMetrics] = useState({
    totalTransactions: 0,
    reconciled: 0,
    exceptions: 0,
    pendingUploads: 0,
    reconciledAmount: 0,
    pendingAmount: 0,
  });

  useEffect(() => {
    // Only fetch from succeeded_matches
    fetch("http://localhost:5001/api/transactions?table=succeeded_matches")
      .then((response) => response.json())
      .then((succeededData) => {
        // Calculate 90 days ago
        const now = new Date();
        const ninetyDaysAgo = new Date(now.setDate(now.getDate() - 90));

        // Filter and sum by merge_source
        const recentTransactions = succeededData.filter((tx) => {
          const txDate = new Date(tx.stripe_created_date_utc);
          return txDate >= ninetyDaysAgo;
        });

        const amountsBySource = recentTransactions.reduce((acc, tx) => {
          const source = tx.merge_source;
          const amount = parseFloat(tx.stripe_converted_amount) || 0;

          // Log each transaction's details
          console.log("Transaction:", {
            id: tx.stripe_paymentintent_id,
            source: source,
            rawAmount: tx.stripe_converted_amount,
            parsedAmount: amount,
            currency: tx.stripe_converted_currency,
            date: tx.stripe_created_date_utc,
          });

          acc[source] = (acc[source] || 0) + amount;
          return acc;
        }, {});

        // Log totals by merge_source
        console.log("Total amounts by merge_source (last 90 days):", {
          ...amountsBySource,
          dateRange: {
            from: ninetyDaysAgo.toISOString(),
            to: new Date().toISOString(),
          },
        });

        setMetrics({
          totalTransactions: succeededData.length,
          reconciled: succeededData.filter((tx) => tx.merge_source === "match")
            .length,
          exceptions: succeededData.filter(
            (tx) => tx.merge_source === "stripe_only"
          ).length,
          pendingUploads: 0,
          reconciledAmount: amountsBySource["match"] || 0,
          pendingAmount: amountsBySource["stripe_only"] || 0,
        });
      })
      .catch((error) => console.error("Error fetching metrics:", error));
  }, []);

  return (
    <div className="home-container">
      <div className="welcome-section">
        <h1>Hi, welcome back 👋</h1>
        <p className="reconciliation-status">
          $
          {metrics.reconciledAmount.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}{" "}
          have been reconciled in the last 90 days
        </p>
        <p className="pending-status">
          $
          {metrics.pendingAmount.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })}{" "}
          still pending reconciliation
        </p>
      </div>

      <div className="menu-grid">
        <Link to="/dashboard" className="menu-card">
          <div className="icon-container">
            <MdDashboard className="menu-icon" />
          </div>
          <div className="card-content">
            <h3>Dashboard</h3>
            <p>View reconciliation metrics and analytics</p>
            <div className="metric">{metrics.totalTransactions}</div>
            <div className="metric-label">Total Transactions</div>
          </div>
        </Link>

        <Link to="/transactions" className="menu-card">
          <div className="icon-container">
            <MdPayments className="menu-icon" />
          </div>
          <div className="card-content">
            <h3>Transactions</h3>
            <p>Manage and reconcile your transactions</p>
            <div className="metric">{metrics.reconciled}</div>
            <div className="metric-label">Reconciled</div>
          </div>
        </Link>

        <Link to="/reports" className="menu-card">
          <div className="icon-container">
            <MdAssessment className="menu-icon" />
          </div>
          <div className="card-content">
            <h3>Reports</h3>
            <p>Generate and view reconciliation reports</p>
            <div className="metric">{metrics.exceptions}</div>
            <div className="metric-label">Exceptions</div>
          </div>
        </Link>

        <Link to="/data-input" className="menu-card">
          <div className="icon-container">
            <MdCloudUpload className="menu-icon" />
          </div>
          <div className="card-content">
            <h3>Data Input</h3>
            <p>Upload and manage your data sources</p>
            <div className="metric">{metrics.pendingUploads}</div>
            <div className="metric-label">Pending Uploads</div>
          </div>
        </Link>
      </div>
    </div>
  );
}

export default Home;

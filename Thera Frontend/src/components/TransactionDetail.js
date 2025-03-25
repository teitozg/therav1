import React from "react";
import { useParams, useLocation, useNavigate } from "react-router-dom";
import "./TransactionDetail.css";

function TransactionDetail() {
  const { id } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const transaction = location.state?.transaction;

  if (!transaction) {
    return <div>Transaction not found</div>;
  }

  return (
    <div className="transaction-detail">
      <div className="detail-header">
        <button className="back-button" onClick={() => navigate(-1)}>
          ← Back to Transactions
        </button>
        <h2>Transaction Details</h2>
      </div>

      <div className="detail-content">
        <div className="detail-section">
          <h3>Basic Information</h3>
          <div className="detail-grid">
            <div className="detail-item">
              <label>Date</label>
              <span>
                {new Date(
                  transaction.stripe_created_date_utc
                ).toLocaleDateString()}
              </span>
            </div>
            <div className="detail-item">
              <label>Amount</label>
              <span>{transaction.stripe_converted_amount}</span>
            </div>
            <div className="detail-item">
              <label>Currency</label>
              <span>{transaction.stripe_converted_currency}</span>
            </div>
            <div className="detail-item">
              <label>Status</label>
              <span className={`status ${transaction.merge_source}`}>
                {transaction.merge_source}
              </span>
            </div>
          </div>
        </div>

        <div className="detail-section">
          <h3>Customer Information</h3>
          <div className="detail-grid">
            <div className="detail-item">
              <label>Customer ID</label>
              <span>{transaction.stripe_customer_id}</span>
            </div>
            <div className="detail-item">
              <label>Customer Email</label>
              <span>{transaction.stripe_customer_email}</span>
            </div>
          </div>
        </div>

        <div className="detail-section">
          <h3>Payment Information</h3>
          <div className="detail-grid">
            <div className="detail-item">
              <label>PaymentIntent ID</label>
              <span>{transaction.stripe_paymentintent_id}</span>
            </div>
            <div className="detail-item">
              <label>Stripe ID</label>
              <span>{transaction.stripe_id}</span>
            </div>
            <div className="detail-item">
              <label>Mode</label>
              <span>{transaction.stripe_mode || "N/A"}</span>
            </div>
            <div className="detail-item">
              <label>Payment Source Type</label>
              <span>{transaction.stripe_payment_source_type || "N/A"}</span>
            </div>
            <div className="detail-item">
              <label>Card Brand</label>
              <span>{transaction.stripe_card_brand || "N/A"}</span>
            </div>
            <div className="detail-item">
              <label>Fee</label>
              <span>{transaction.stripe_fee || "N/A"}</span>
            </div>
            <div className="detail-item">
              <label>Seller Message</label>
              <span>{transaction.stripe_seller_message || "N/A"}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default TransactionDetail;

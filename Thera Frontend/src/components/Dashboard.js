import React from "react";
import "../styles/common.css";

function Dashboard() {
  return (
    <div className="page-container">
      <h2>Reconciliation Dashboard</h2>
      <div className="dashboard-container">
        <iframe
          width="100%"
          height="800"
          src="https://lookerstudio.google.com/embed/reporting/bb1feddc-579f-4f11-aadd-cc966b227720/page/bR1AF"
          frameBorder="0"
          style={{ border: 0, borderRadius: "8px" }}
          allowFullScreen
          sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"
        />
      </div>
    </div>
  );
}

export default Dashboard;

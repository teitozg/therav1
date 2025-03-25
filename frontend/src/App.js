import React from "react";
import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";
import Transactions from "./pages/Transactions";
import "./App.css";

function App() {
  return (
    <Router>
      <div className="App">
        <div className="sidebar">
          <div className="logo">
            <span className="T">T</span>
            <span className="logo-text">Thera</span>
          </div>
          <nav>
            <Link to="/">
              <span>🏠</span> Home
            </Link>
            <Link to="/dashboard">
              <span>📊</span> Dashboard
            </Link>
            <Link to="/transactions" className="active">
              <span>💳</span> Transactions
            </Link>
            <Link to="/reports">
              <span>📄</span> Reports
            </Link>
            <Link to="/data-input">
              <span>📥</span> Data Input
            </Link>
          </nav>
          <div className="bottom-nav">
            <Link to="/settings">
              <span>⚙️</span> Settings
            </Link>
            <Link to="/profile">
              <span>👤</span> Profile
            </Link>
          </div>
        </div>
        <Routes>
          <Route path="/transactions" element={<Transactions />} />
          {/* Add other routes here */}
        </Routes>
      </div>
    </Router>
  );
}

export default App;

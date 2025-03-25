import React from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import Home from "./components/Home";
import Dashboard from "./components/Dashboard";
import DataInput from "./components/DataInput";
import Reports from "./components/Reports";
import Transactions from "./components/Transactions";
import TransactionDetail from "./components/TransactionDetail";
import "./App.css";

function App() {
  return (
    <Router>
      <div className="App">
        <Sidebar />
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/transactions" element={<Transactions />} />
          <Route path="/transactions/:id" element={<TransactionDetail />} />
          <Route path="/data-input" element={<DataInput />} />
          <Route path="/sources/:sourceId/upload" element={<DataInput />} />
          <Route path="/reports" element={<Reports />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;

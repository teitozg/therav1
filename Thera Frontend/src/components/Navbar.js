import React from "react";
import { Link } from "react-router-dom";
import { Nav } from "react-bootstrap";

function Navbar() {
  return (
    <nav className="navbar">
      <div className="nav-brand">
        <Link to="/">Thera</Link>
      </div>
      <div className="nav-links">
        <Link to="/dashboard">Dashboard</Link>
        <Link to="/transactions">Transactions</Link>
        <Nav.Link as={Link} to="/reports">
          Reports
        </Nav.Link>
        <Link to="/data-input">Data Input</Link>
      </div>
    </nav>
  );
}

export default Navbar;

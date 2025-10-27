import numpy as np
import pandas as pd

#Geometric Brownian Motion
def simulate_balance_by_account_level(
    level="Gold level",
    start_date="2022-01-01",
    days=252,
    seed=None
):
    """
    Simulates account balance variation over time based on account level.
    Returns a list of (date, balance).
    """
    ACCOUNT_LEVEL_PROFILES = {
    "Basic level":    {"start_balance": 500,   "mu": -0.0001, "sigma": 0.02},
    "Silver level":   {"start_balance": 1500,  "mu": 0.0002,  "sigma": 0.015},
    "Gold level":     {"start_balance": 3000,  "mu": 0.0004,  "sigma": 0.01},
    "Platinum level": {"start_balance": 6000,  "mu": 0.0005,  "sigma": 0.008},
    "Elite level":    {"start_balance": 12000, "mu": 0.0003,  "sigma": 0.005},
    "Diamond level":  {"start_balance": 25000, "mu": 0.0002,  "sigma": 0.004},
    }

    if level not in ACCOUNT_LEVEL_PROFILES:
        raise ValueError(f"Unknown account level: {level}")

    profile = ACCOUNT_LEVEL_PROFILES[level]
    start_balance = profile["start_balance"]
    mu = profile["mu"]
    sigma = profile["sigma"]

    if seed is not None:
        np.random.seed(seed)

    dt = 1 / 252
    shocks = np.random.normal(0, 1, days)
    returns = (mu - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * shocks

    balances = [start_balance]
    for r in returns:
        new_balance = balances[-1] * np.exp(r)
        balances.append(new_balance)

    dates = pd.date_range(start=start_date, periods=days, freq="B")
    return list(zip(dates, balances))

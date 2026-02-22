"""Pydantic models for The Odds API responses."""

from __future__ import annotations

from pydantic import BaseModel


class OutcomeSchema(BaseModel):
    name: str
    price: float
    point: float | None = None
    link: str | None = None


class MarketSchema(BaseModel):
    key: str
    outcomes: list[OutcomeSchema]
    link: str | None = None


class BookmakerSchema(BaseModel):
    key: str
    title: str
    markets: list[MarketSchema]
    link: str | None = None


class EventOddsSchema(BaseModel):
    id: str
    sport_key: str
    home_team: str
    away_team: str
    commence_time: str
    bookmakers: list[BookmakerSchema]


class SportSchema(BaseModel):
    key: str
    active: bool
    title: str
    has_outrights: bool

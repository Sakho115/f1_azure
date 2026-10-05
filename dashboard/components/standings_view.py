"""
Streamlit Component: Driver and Constructor Championship Standings View.
Renders interactive Plotly charts, points trajectories, and podium summaries.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def render_standings_view(driver_standings_df: pd.DataFrame, constructor_standings_df: pd.DataFrame):
    st.header("🏆 Formula 1 Championship Standings")

    # Available seasons
    seasons = sorted(driver_standings_df["race_year"].unique(), reverse=True)
    selected_season = st.selectbox("Select Formula 1 Season:", seasons, key="standings_season_select")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Drivers Championship ({selected_season})")
        season_drivers = driver_standings_df[driver_standings_df["race_year"] == selected_season].sort_values("rank")

        # Top 3 metric cards
        if not season_drivers.empty:
            m1, m2, m3 = st.columns(3)
            p1 = season_drivers.iloc[0]
            m1.metric("🥇 Champion", f"{p1['full_name']}", f"{p1['total_points']} pts ({p1['wins']} wins)")
            if len(season_drivers) > 1:
                p2 = season_drivers.iloc[1]
                m2.metric("🥈 P2", f"{p2['full_name']}", f"{p2['total_points']} pts")
            if len(season_drivers) > 2:
                p3 = season_drivers.iloc[2]
                m3.metric("🥉 P3", f"{p3['full_name']}", f"{p3['total_points']} pts")

        fig_drivers = px.bar(
            season_drivers,
            x="total_points",
            y="full_name",
            orientation="h",
            color="team_name",
            title=f"Driver Points Leaderboard - {selected_season}",
            labels={"total_points": "Championship Points", "full_name": "Driver", "team_name": "Constructor"},
            text="total_points"
        )
        fig_drivers.update_layout(yaxis={"categoryorder": "total ascending"}, height=450)
        st.plotly_chart(fig_drivers, use_container_width=True)

        driver_cols = [c for c in ["rank", "full_name", "driver_nationality", "nationality", "team_name", "total_points", "wins", "podiums"] if c in season_drivers.columns]
        st.dataframe(
            season_drivers[driver_cols],
            use_container_width=True,
            hide_index=True
        )

    with col2:
        st.subheader(f"Constructors Championship ({selected_season})")
        season_teams = constructor_standings_df[constructor_standings_df["race_year"] == selected_season].sort_values("rank")

        if not season_teams.empty:
            team1 = season_teams.iloc[0]
            st.metric("🏆 Constructors Champion", f"{team1['team_name']}", f"{team1['total_points']} pts")

        fig_teams = px.pie(
            season_teams,
            names="team_name",
            values="total_points",
            title=f"Constructor Points Share - {selected_season}",
            hole=0.4
        )
        fig_teams.update_layout(height=450)
        st.plotly_chart(fig_teams, use_container_width=True)

        team_cols = [c for c in ["rank", "team_name", "team_nationality", "nationality", "total_points", "wins", "podiums"] if c in season_teams.columns]
        st.dataframe(
            season_teams[team_cols],
            use_container_width=True,
            hide_index=True
        )

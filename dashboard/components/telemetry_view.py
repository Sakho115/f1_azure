"""
Streamlit Component: Race Telemetry, Pit Stop Efficiency, and Qualifying View.
"""

import streamlit as st
import plotly.express as px
import pandas as pd

def render_telemetry_view(
    results_df: pd.DataFrame,
    pitstops_df: pd.DataFrame,
    qualifying_df: pd.DataFrame
):
    st.header("⚡ Race Telemetry, Pit Stops & Qualifying Pace")

    tab1, tab2, tab3 = st.tabs(["🏁 Race Results & Overtakes", "🛠️ Pit Stop Performance", "⏱️ Qualifying vs Race"])

    with tab1:
        st.subheader("Race Results & Position Changes")
        races = sorted(results_df["race_name"].unique())
        selected_race = st.selectbox("Select Grand Prix:", races, key="telemetry_race_select")

        race_data = results_df[results_df["race_name"] == selected_race].sort_values("position_order")

        # Scatter plot: Grid vs Finish position
        fig_overtakes = px.scatter(
            race_data,
            x="grid",
            y="position",
            color="team_name",
            size="points",
            hover_name="driver_name",
            text="driver_code",
            title=f"Starting Grid vs Final Position - {selected_race}",
            labels={"grid": "Grid Starting Position", "position": "Finish Position"}
        )
        # Add diagonal line (where starting position == finish position)
        fig_overtakes.add_shape(
            type="line", line=dict(dash="dash", color="gray"),
            x0=0, y0=0, x1=20, y1=20
        )
        fig_overtakes.update_traces(textposition="top center")
        st.plotly_chart(fig_overtakes, use_container_width=True)

        st.dataframe(
            race_data[["position", "driver_name", "team_name", "grid", "positions_gained", "points", "race_time", "fastest_lap_speed"]],
            use_container_width=True,
            hide_index=True
        )

    with tab2:
        st.subheader("Team Pit Stop Strategy & Duration Analysis")
        if not pitstops_df.empty:
            fig_pits = px.bar(
                pitstops_df,
                x="team_name",
                y="avg_pit_stop_sec",
                color="team_name",
                error_y="fastest_pit_stop_sec",
                title="Average Pit Stop Duration by Constructor (Seconds)",
                labels={"avg_pit_stop_sec": "Avg Duration (s)", "team_name": "Constructor"}
            )
            st.plotly_chart(fig_pits, use_container_width=True)

            st.dataframe(
                pitstops_df[["race_year", "race_name", "team_name", "total_pit_stops", "avg_pit_stop_sec", "fastest_pit_stop_sec"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No pit stop data available.")

    with tab3:
        st.subheader("Qualifying Conversion & Grid Delta")
        if not qualifying_df.empty:
            fig_quali = px.bar(
                qualifying_df,
                x="driver_name",
                y="grid_delta",
                color="grid_delta",
                color_continuous_scale="RdYlGn",
                title="Net Positions Gained/Lost from Qualifying to Finish",
                labels={"grid_delta": "Net Positions Gained (>0 Gained, <0 Lost)", "driver_name": "Driver"}
            )
            st.plotly_chart(fig_quali, use_container_width=True)

            st.dataframe(
                qualifying_df[["race_year", "race_name", "driver_name", "qualifying_position", "finish_position", "grid_delta", "q1", "q2", "q3"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No qualifying analysis available.")

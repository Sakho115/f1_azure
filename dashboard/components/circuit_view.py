"""
Streamlit Component: Formula 1 World Circuits Map & Track Information.
"""

import streamlit as st
import plotly.express as px
import pandas as pd

def render_circuit_view(circuits_df: pd.DataFrame):
    st.header("🌍 Global Formula 1 Circuits")

    if circuits_df.empty:
        st.info("No circuit data available.")
        return

    col1, col2 = st.columns([2, 1])

    with col1:
        # Plotly Scatter Geo Map
        fig_map = px.scatter_geo(
            circuits_df,
            lat="latitude",
            lon="longitude",
            hover_name="circuit_name",
            hover_data={"location": True, "country": True, "altitude": True, "latitude": False, "longitude": False},
            size=[15] * len(circuits_df),
            color="country",
            title="Formula 1 Grand Prix Locations Worldwide",
            projection="natural earth"
        )
        fig_map.update_layout(height=500, margin={"r": 0, "t": 40, "l": 0, "b": 0})
        st.plotly_chart(fig_map, use_container_width=True)

    with col2:
        st.subheader("Circuit Track Registry")
        st.dataframe(
            circuits_df[["circuit_id", "circuit_name", "location", "country", "altitude"]],
            use_container_width=True,
            hide_index=True
        )

        st.subheader("Circuit Elevation Profiles")
        fig_alt = px.bar(
            circuits_df.sort_values("altitude", ascending=False),
            x="circuit_name",
            y="altitude",
            color="altitude",
            title="Track Altitude Above Sea Level (Meters)",
            labels={"altitude": "Altitude (m)", "circuit_name": "Circuit"}
        )
        st.plotly_chart(fig_alt, use_container_width=True)

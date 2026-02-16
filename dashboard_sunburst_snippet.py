
    # --- Sunburst de Temáticas ---
    if os.path.exists(TOPICS_FILE):
        st.markdown("---")
        st.subheader("Panorama Temático (Sunburst)")
        
        try:
            topics_df = pd.read_parquet(TOPICS_FILE)
            if not topics_df.empty:
                # Agrupar por jerarquía para el gráfico
                # (Sumamos 'count' que es works count asociado al tópico en esa revista)
                sun_df = topics_df.groupby(['domain', 'field', 'topic_name'])['count'].sum().reset_index()
                
                # Filtrar ruidos (opcional)
                sun_df = sun_df[sun_df['count'] > 0]
                
                if not sun_df.empty:
                    fig_sun = px.sunburst(
                        sun_df,
                        path=['domain', 'field', 'topic_name'],
                        values='count',
                        title='Distribución Jerárquica de Temas de Investigación',
                        height=700,
                        color='domain', # Colorear por dominio para consistencia visual
                        color_discrete_sequence=px.colors.qualitative.Prism
                    )
                    fig_sun.update_layout(margin=dict(t=40, l=0, r=0, b=0))
                    
                    st.plotly_chart(fig_sun, use_container_width=True)
                    st.caption("El gráfico muestra la jerarquía de: Dominio -> Campo -> Tópico. El tamaño de cada sector es proporcional al volumen de documentos.")
                else:
                    st.info("No hay datos de temáticas suficientes para mostrar.")
        except Exception as e:
            st.error(f"Error visualizando temáticas: {e}")

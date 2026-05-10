"""
Utilidades de Entrenamiento y Evaluación
=========================================
Funciones para:
- Walk-Forward Validation (Fase 2)
- Métricas de evaluación de clasificación binaria
- Gráficos de diagnóstico (ROC, Precision-Recall, Calibración)
- Explicabilidad con SHAP
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, brier_score_loss,
    roc_curve, precision_recall_curve,
    confusion_matrix, classification_report,
)


# =============================================================================
# 1. WALK-FORWARD VALIDATION (PARTICIÓN TEMPORAL)
# =============================================================================

def walk_forward_split(df: pd.DataFrame,
                       date_col: str = 'date',
                       n_folds: int = 4,
                       val_months: int = 1,
                       min_train_months: int = 3) -> list:
    """
    Genera los índices de Train/Val para Walk-Forward Cross-Validation temporal.

    La ventana de entrenamiento crece progresivamente, validando sobre el bloque
    de meses inmediatamente posterior.

    Parámetros:
    -----------
    df : DataFrame con columna de fecha
    date_col : nombre de la columna datetime
    n_folds : número de pliegues (ventanas) a generar
    val_months : número de meses que dura cada bloque de validación
    min_train_months : mínimo de meses requeridos en el primer fold de entrenamiento

    Devuelve:
    ---------
    Lista de tuplas (train_idx, val_idx) con los índices de cada fold.
    """
    # Trabajar solo con la Serie de fechas para no duplicar el DataFrame completo en RAM
    date_series = pd.to_datetime(df[date_col])
    periods = date_series.dt.to_period('M')

    # Obtener meses únicos ordenados
    months = pd.PeriodIndex(periods.unique()).sort_values()

    folds = []
    for i in range(n_folds):
        # Índice del primer mes de validación
        val_start_idx = min_train_months + i * val_months
        val_end_idx = val_start_idx + val_months

        if val_end_idx > len(months):
            break

        train_months = months[:val_start_idx]
        val_months_set = months[val_start_idx:val_end_idx]

        train_mask = periods.isin(train_months)
        val_mask = periods.isin(val_months_set)

        train_idx = df.index[train_mask]
        val_idx = df.index[val_mask]

        if len(train_idx) > 0 and len(val_idx) > 0:
            folds.append((train_idx, val_idx))
            print(f"  Fold {len(folds)}: Train [{train_months[0]}..{train_months[-1]}] "
                  f"({len(train_idx):,} rows) | "
                  f"Val [{val_months_set[0]}..{val_months_set[-1]}] "
                  f"({len(val_idx):,} rows)")

    print(f"[Walk-Forward] Generados {len(folds)} folds temporales.")
    return folds


# =============================================================================
# 2. MÉTRICAS DE EVALUACIÓN
# =============================================================================

def evaluate_classification(y_true: np.ndarray, y_pred: np.ndarray,
                            y_prob: np.ndarray = None,
                            fold_name: str = '') -> dict:
    """
    Calcula métricas completas de clasificación binaria.
    Devuelve un diccionario con las métricas para almacenamiento/comparación.
    """
    metrics = {
        'fold': fold_name,
        'accuracy': accuracy_score(y_true, y_pred),
        'precision': precision_score(y_true, y_pred, zero_division=0),
        'recall': recall_score(y_true, y_pred, zero_division=0),
        'f1': f1_score(y_true, y_pred, zero_division=0),
    }

    if y_prob is not None:
        metrics['roc_auc'] = roc_auc_score(y_true, y_prob)
        metrics['avg_precision'] = average_precision_score(y_true, y_prob)
        metrics['brier_score'] = brier_score_loss(y_true, y_prob)

    print(f"\n📊 Métricas [{fold_name}]:")
    for k, v in metrics.items():
        if k != 'fold':
            print(f"   {k:>15}: {v:.4f}")

    return metrics


def aggregate_fold_metrics(metrics_list: list) -> pd.DataFrame:
    """
    Agrega métricas de todos los folds del Walk-Forward en un resumen.
    """
    df = pd.DataFrame(metrics_list)
    summary = df.describe().loc[['mean', 'std']].round(4)
    print("\n📋 Resumen Walk-Forward CV:")
    print(summary.to_string())
    return df


# =============================================================================
# 3. GRÁFICOS DE DIAGNÓSTICO
# =============================================================================

def plot_roc_and_pr_curves(y_true: np.ndarray, y_prob: np.ndarray,
                           title_suffix: str = ''):
    """
    Grafica la curva ROC y la curva Precision-Recall en un mismo panel.
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # ROC
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    auc_val = roc_auc_score(y_true, y_prob)
    axes[0].plot(fpr, tpr, color='darkorange', lw=2, label=f'AUC = {auc_val:.4f}')
    axes[0].plot([0, 1], [0, 1], color='gray', linestyle='--', lw=1)
    axes[0].set_xlabel('False Positive Rate')
    axes[0].set_ylabel('True Positive Rate')
    axes[0].set_title(f'Curva ROC {title_suffix}')
    axes[0].legend(loc='lower right')
    axes[0].grid(True, alpha=0.3)

    # Precision-Recall
    precision, recall, _ = precision_recall_curve(y_true, y_prob)
    ap = average_precision_score(y_true, y_prob)
    axes[1].plot(recall, precision, color='teal', lw=2, label=f'AP = {ap:.4f}')
    baseline = y_true.mean()
    axes[1].axhline(baseline, color='gray', linestyle='--', lw=1, label=f'Baseline = {baseline:.2f}')
    axes[1].set_xlabel('Recall')
    axes[1].set_ylabel('Precision')
    axes[1].set_title(f'Curva Precision-Recall {title_suffix}')
    axes[1].legend(loc='upper right')
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()


def plot_confusion_matrix(y_true: np.ndarray, y_pred: np.ndarray,
                          title_suffix: str = '', ax=None):
    """
    Grafica la matriz de confusión normalizada.
    Si se pasa un eje 'ax', dibuja en él; si no, crea una figura nueva.
    """
    from sklearn.metrics import confusion_matrix
    import seaborn as sns
    import matplotlib.pyplot as plt

    cm = confusion_matrix(y_true, y_pred, normalize='true')

    if ax is None:
        fig, ax = plt.subplots(figsize=(6, 5))

    sns.heatmap(cm, annot=True, fmt='.2%', cmap='Blues', ax=ax,
                xticklabels=['Vacío (0)', 'Ocupado (1)'],
                yticklabels=['Vacío (0)', 'Ocupado (1)'],
                cbar=False)
    ax.set_xlabel('Predicción')
    ax.set_ylabel('Real')
    ax.set_title(f'Matriz de Confusión {title_suffix}')
    
    if ax is None:
        plt.tight_layout()
        plt.show()


# =============================================================================
# 4. EXPLICABILIDAD CON SHAP
# =============================================================================

def _get_shap_explainer_and_values(model, X: pd.DataFrame):
    """
    Helper interno: selecciona el Explainer correcto según el tipo de modelo
    y devuelve los SHAP values como un array numpy 2D.

    - Árboles (feature_importances_): TreeExplainer (exacto, rápido).
    - Lineales (coef_): LinearExplainer (exacto, milisegundos).

    Si se recibe un sklearn Pipeline, desempaqueta automáticamente el estimador
    final (model[-1]) para acceder a coef_ / feature_importances_.

    Compatible con SHAP 0.40+ (API de Explanation objects).
    """
    import shap
    from sklearn.pipeline import Pipeline

    # Desempaquetar el estimador final si es un Pipeline de sklearn
    estimator = model[-1] if isinstance(model, Pipeline) else model
    model_type = type(estimator).__name__

    if hasattr(estimator, 'feature_importances_'):
        # Árboles: XGBoost, LightGBM, RandomForest
        explainer = shap.TreeExplainer(estimator)
    elif hasattr(estimator, 'coef_'):
        # Lineales: LogisticRegression, LinearRegression, SGD
        masker = shap.maskers.Independent(X, max_samples=100)
        explainer = shap.LinearExplainer(estimator, masker)
    else:
        raise ValueError(
            f"[SHAP] Tipo de modelo no soportado: {model_type}. "
            "Se esperaba un modelo con 'feature_importances_' o 'coef_'."
        )

    # SHAP 0.40+ devuelve un Explanation object, no una lista
    explanation = explainer(X)

    # Para clasificación binaria, algunos explainers devuelven shape (n, features, 2).
    # Hacemos el slicing sobre el Explanation object completo (no solo el array),
    # para que plot_shap_summary reciba un Explanation 2D limpio y no crashee.
    if explanation.values.ndim == 3:
        explanation = explanation[:, :, 1]  # Clase positiva

    shap_vals = explanation.values

    return explanation, shap_vals


def plot_shap_summary(model, X: pd.DataFrame, max_display: int = 20):
    """
    Genera el SHAP summary plot (beeswarm) para evaluar la relevancia
    de las features generadas por el pipeline.

    Requiere: pip install shap
    """
    try:
        import shap
    except ImportError:
        print("[SHAP] Librería 'shap' no instalada. Ejecuta: pip install shap")
        return

    model_type = type(model).__name__
    print(f"[SHAP] Calculando valores SHAP para {X.shape[1]} features ({model_type})...")

    explanation, _ = _get_shap_explainer_and_values(model, X)
    shap.summary_plot(explanation, X, max_display=max_display, show=True)

    print(f"[SHAP] Summary plot generado para modelo: {model_type}")


def get_shap_feature_importance(model, X: pd.DataFrame) -> pd.Series:
    """
    Devuelve la importancia media absoluta de SHAP como un pd.Series ordenado.
    Útil para comparar features entre pipelines.
    """
    try:
        import shap
    except ImportError:
        print("[SHAP] Librería 'shap' no instalada.")
        return pd.Series(dtype=float)

    _, shap_vals = _get_shap_explainer_and_values(model, X)

    importance = pd.Series(
        np.abs(shap_vals).mean(axis=0),
        index=X.columns
    ).sort_values(ascending=False)

    return importance

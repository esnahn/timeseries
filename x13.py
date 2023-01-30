# "Unfortunately, we no longer have someone familiar with X13 actively working on
#  Statsmodels, so we might be of limited help to you."
#                                                           - ChadFulton
# https://github.com/statsmodels/statsmodels/issues/6485

import os
import re
import tempfile
from warnings import warn

import pandas as pd
from statsmodels.tools.sm_exceptions import (
    IOWarning,
    X13Error,
    X13NotFoundError,
    X13Warning,
)
from statsmodels.tsa.x13 import (
    # X13ArimaAnalysisResult,
    _check_errors,
    _check_x12,
    _convert_out_to_series,
    _log_to_x12,
    _make_automdl_options,
    _make_forecast_options,
    _make_regression_options,
    _open_and_read,
    pandas_to_series_spec,
    run_spec,
    x13_arima_select_order,
    _clean_order,
)
from statsmodels.tools.tools import Bunch


def x13_arima_analysis(
    endog,
    maxorder=(2, 1),
    maxdiff=(2, 1),
    diff=None,
    exog=None,
    log=None,
    outlier=True,
    trading=False,
    forecast_periods=None,
    retspec=False,
    speconly=False,
    start=None,
    freq=None,
    print_stdout=False,
    x12path=None,
    prefer_x13=True,
):
    """
    Perform x13-arima analysis for monthly or quarterly data.

    Parameters
    ----------
    endog : array_like, pandas.Series
        The series to model. It is best to use a pandas object with a
        DatetimeIndex or PeriodIndex. However, you can pass an array-like
        object. If your object does not have a dates index then ``start`` and
        ``freq`` are not optional.
    maxorder : tuple
        The maximum order of the regular and seasonal ARMA polynomials to
        examine during the model identification. The order for the regular
        polynomial must be greater than zero and no larger than 4. The
        order for the seasonal polynomial may be 1 or 2.
    maxdiff : tuple
        The maximum orders for regular and seasonal differencing in the
        automatic differencing procedure. Acceptable inputs for regular
        differencing are 1 and 2. The maximum order for seasonal differencing
        is 1. If ``diff`` is specified then ``maxdiff`` should be None.
        Otherwise, ``diff`` will be ignored. See also ``diff``.
    diff : tuple
        Fixes the orders of differencing for the regular and seasonal
        differencing. Regular differencing may be 0, 1, or 2. Seasonal
        differencing may be 0 or 1. ``maxdiff`` must be None, otherwise
        ``diff`` is ignored.
    exog : array_like
        Exogenous variables.
    log : bool or None
        If None, it is automatically determined whether to log the series or
        not. If False, logs are not taken. If True, logs are taken.
    outlier : bool
        Whether or not outliers are tested for and corrected, if detected.
    trading : bool
        Whether or not trading day effects are tested for.
    forecast_periods : int
        Number of forecasts produced. The default is None.
    retspec : bool
        Whether to return the created specification file. Can be useful for
        debugging.
    speconly : bool
        Whether to create the specification file and then return it without
        performing the analysis. Can be useful for debugging.
    start : str, datetime
        Must be given if ``endog`` does not have date information in its index.
        Anything accepted by pandas.DatetimeIndex for the start value.
    freq : str
        Must be givein if ``endog`` does not have date information in its
        index. Anything accepted by pandas.DatetimeIndex for the freq value.
    print_stdout : bool
        The stdout from X12/X13 is suppressed. To print it out, set this
        to True. Default is False.
    x12path : str or None
        The path to x12 or x13 binary. If None, the program will attempt
        to find x13as or x12a on the PATH or by looking at X13PATH or
        X12PATH depending on the value of prefer_x13.
    prefer_x13 : bool
        If True, will look for x13as first and will fallback to the X13PATH
        environmental variable. If False, will look for x12a first and will
        fallback to the X12PATH environmental variable. If x12path points
        to the path for the X12/X13 binary, it does nothing.

    Returns
    -------
    Bunch
        A bunch object containing the listed attributes.

        - results : str
          The full output from the X12/X13 run.
        - seasadj : pandas.Series
          The final seasonally adjusted ``endog``.
        - trend : pandas.Series
          The trend-cycle component of ``endog``.
        - irregular : pandas.Series
          The final irregular component of ``endog``.
        - stdout : str
          The captured stdout produced by x12/x13.
        - spec : str, optional
          Returned if ``retspec`` is True. The only thing returned if
          ``speconly`` is True.

    Notes
    -----
    This works by creating a specification file, writing it to a temporary
    directory, invoking X12/X13 in a subprocess, and reading the output
    directory, invoking exog12/X13 in a subprocess, and reading the output
    back in.
    """
    x12path = _check_x12(x12path)

    if not isinstance(endog, (pd.DataFrame, pd.Series)):
        if start is None or freq is None:
            raise ValueError(
                "start and freq cannot be none if endog is not " "a pandas object"
            )
        endog = pd.Series(
            endog,
            index=pd.DatetimeIndex(start=start, periods=len(endog), freq=freq),
        )
    spec_obj = pandas_to_series_spec(endog)
    spec = spec_obj.create_spec()
    spec += "transform{{function={0}}}\n".format(_log_to_x12[log])
    if outlier:
        spec += "outlier{}\n"
    options = _make_automdl_options(maxorder, maxdiff, diff)
    spec += "automdl{{{0}}}\n".format(options)
    spec += _make_regression_options(trading, exog)
    spec += _make_forecast_options(forecast_periods)
    spec += "x11{ save=(d10 d11 d12 d13) }"  # add seasonal
    if speconly:
        return spec
    # write it to a tempfile
    # TODO: make this more robust - give the user some control?
    ftempin = tempfile.NamedTemporaryFile(delete=False, suffix=".spc")
    ftempout = tempfile.NamedTemporaryFile(delete=False)
    try:
        ftempin.write(spec.encode("utf8"))
        ftempin.close()
        ftempout.close()
        # call x12 arima
        p = run_spec(x12path, ftempin.name[:-4], ftempout.name)
        p.wait()
        stdout = p.stdout.read()
        if print_stdout:
            print(p.stdout.read())
        # check for errors
        errors = _open_and_read(ftempout.name + ".err")
        _check_errors(errors)

        # read in results
        results = _open_and_read(ftempout.name + ".out")
        seasadj = _open_and_read(ftempout.name + ".d11")
        trend = _open_and_read(ftempout.name + ".d12")
        seasonal = _open_and_read(ftempout.name + ".d10")  # add seasonal
        irregular = _open_and_read(ftempout.name + ".d13")
    finally:
        try:  # sometimes this gives a permission denied error?
            #   not sure why. no process should have these open
            os.remove(ftempin.name)
            os.remove(ftempout.name)
        except OSError:
            if os.path.exists(ftempin.name):
                warn(
                    "Failed to delete resource {0}".format(ftempin.name),
                    IOWarning,
                )
            if os.path.exists(ftempout.name):
                warn(
                    "Failed to delete resource {0}".format(ftempout.name),
                    IOWarning,
                )

    seasadj = _convert_out_to_series(seasadj, endog.index, "seasadj")
    trend = _convert_out_to_series(trend, endog.index, "trend")
    seasonal = _convert_out_to_series(seasonal, endog.index, "seasonal")  # add seasonal
    irregular = _convert_out_to_series(irregular, endog.index, "irregular")

    # NOTE: there is not likely anything in stdout that's not in results
    #       so may be safe to just suppress and remove it
    if not retspec:
        res = X13ArimaAnalysisResult(
            observed=endog,
            results=results,
            seasadj=seasadj,
            trend=trend,
            seasonal=seasonal,  # add seasonal
            irregular=irregular,
            stdout=stdout,
        )
    else:
        res = X13ArimaAnalysisResult(
            observed=endog,
            results=results,
            seasadj=seasadj,
            trend=trend,
            seasonal=seasonal,  # add seasonal
            irregular=irregular,
            stdout=stdout,
            spec=spec,
        )
    return res


def get_arima_order_from_results(results):
    """
    Perform automatic seasonal ARIMA order identification using x12/x13 ARIMA.

    Parameters
    ----------
    results : X13ArimaAnalysisResult
        An output from x13_arima_analysis.

    Returns
    -------
    Bunch
        A bunch object containing the listed attributes.

        - order : tuple
          The regular order.
        - sorder : tuple
          The seasonal order.
        - include_mean : bool
          Whether to include a mean or not.
        - results : str
          The full results from the X12/X13 analysis.
        - stdout : str
          The captured stdout from the X12/X13 analysis.

    Notes
    -----
    This works by creating a specification file, writing it to a temporary
    directory, invoking X12/X13 in a subprocess, and reading the output back
    in.
    """

    if not isinstance(results, X13ArimaAnalysisResult) or not hasattr(
        results, "results"
    ):
        raise ValueError(repr(results))

    model = re.search("(?<=Final automatic model choice : ).*", results.results)
    order = model.group()
    if re.search("Mean is not significant", results.results):
        include_mean = False
    elif re.search("Constant", results.results):
        include_mean = True
    else:
        include_mean = False
    order, sorder = _clean_order(order)
    res = Bunch(
        order=order,
        sorder=sorder,
        include_mean=include_mean,
        results=results.results,
        stdout=results.stdout,
    )
    return res


class X13ArimaAnalysisResult(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def plot(self):
        from statsmodels.graphics.utils import _import_mpl

        plt = _import_mpl()
        fig, axes = plt.subplots(5, 1, sharex=True)
        self.observed.plot(ax=axes[0], legend=False)
        axes[0].set_ylabel("Observed")
        self.seasadj.plot(ax=axes[1], legend=False)
        axes[1].set_ylabel("Seas. Adjusted")
        self.trend.plot(ax=axes[2], legend=False)
        axes[2].set_ylabel("Trend")
        self.seasonal.plot(ax=axes[3], legend=False)  # add seasonal
        axes[3].set_ylabel("Seasonal")
        self.irregular.plot(ax=axes[4], legend=False)
        axes[4].set_ylabel("Irregular")

        fig.tight_layout()
        return fig

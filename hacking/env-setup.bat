@echo off
pushd %~dp0..
    set PYTHONPATH=%CD%\lib;%CD%\test
    set PATH=%CD%\bin;%PATH%
    set MANPATH=%CD%\docs\man
popd

echo PYTHONPATH %PYTHONPATH%
echo PATH %PATH%
echo MANPATH %MANPATH%
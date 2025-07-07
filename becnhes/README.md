# Бенчмарки

В этой директории собраны попытки сделать сравнительное тестирование
между разными фреймворками в максимально похожих условиях:
- все реальные запросы заменяются на Mock с тестовыми данными, 
  чтобы исключить влияние сети и драйвера,
- достаточное количество запусков, чтобы прогреть кеш.


Результаты:

SQLAlchemy (5000 строк):
Finished in: 0.15210652351379395
Objects mapped: 1667
Finished in: 0.06719422340393066
Objects mapped: 1667
Finished in: 0.0664663314819336
Objects mapped: 1667
Finished in: 0.06922745704650879
Objects mapped: 1667
Finished in: 0.11696887016296387
Objects mapped: 1667
Finished in: 0.08510136604309082
Objects mapped: 1667
Finished in: 0.10288429260253906
Objects mapped: 1667
Finished in: 0.0822300910949707
Objects mapped: 1667
Finished in: 0.11336684226989746
Objects mapped: 1667
Finished in: 0.08387231826782227
Objects mapped: 1667

classic-sql-tools (5000 строк):
Finished in 0.11580324172973633s
Objects mapped: 1667
Finished in 0.10777664184570312s
Objects mapped: 1667
Finished in 0.10760378837585449s
Objects mapped: 1667
Finished in 0.10302543640136719s
Objects mapped: 1667
Finished in 0.09660696983337402s
Objects mapped: 1667
Finished in 0.09411787986755371s
Objects mapped: 1667
Finished in 0.11640620231628418s
Objects mapped: 1667
Finished in 0.09402799606323242s
Objects mapped: 1667
Finished in 0.09544086456298828s
Objects mapped: 1667
Finished in 0.09716582298278809s
Objects mapped: 1667

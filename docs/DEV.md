# Developer documentation

## Build

To build the extension, run `mvn package -DskipTests -Dmaven.test.skip=true` and you'll get a file in `target` like this:


```bash
sdk use java 8.0.345-tem && mvn package -DskipTests -Dmaven.test.skip=true
```

```
[INFO] Building druid-multi-stage-query 29.0.1
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  52.736 s
[INFO] Finished at: 2024-07-11T12:51:32+02:00
[INFO] ------------------------------------------------------------------------
```

druid-multi-stage-query-29.0.1.jar should be created in the target directory.

Replace `$DRUID_HOME/extensions/druid-multi-stage-query/druid-multi-stage-query-29.0.1.jar` with created jar.

Restart Druid.

To display query results and stage info of native-query-controller tasks on the web-ui, patched web-console should be used.
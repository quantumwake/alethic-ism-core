#htpasswd -c superse-basic-auth superset 3ccd580658ca4185
htpasswd -c superset-basic-auth superset

kubectl delete secret superse-basic-auth --namespace superset
kubectl create secret generic superset-basic-auth --from-file=superset-basic-auth --namespace superset

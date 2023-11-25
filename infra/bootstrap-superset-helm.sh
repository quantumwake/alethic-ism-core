echo "find the helm chart and install it either something like minikube or a production environment"

kubectl create namespace superset
helm repo add superset http://apache.github.io/superset/
helm install superset --namespace superset superset/superset



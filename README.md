* Add your code in `src/main/`
* Test your code with `src/tests/`
* Package your artifacts
* Modify dockerfile if needed
* Build and push docker image
* Deploy infrastructure with terraform
```
az login
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
....
terraform destroy
```
* Launch Spark app in cluster mode on AKS
```
az aks get-credentials --name aks-sparkbasic-westeurope  --resource-group rg-sparkbasic-westeurope
spark-submit \
    --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
    --deploy-mode cluster \
    --name sparkbasics \
    --conf spark.kubernetes.container.image=avpash43/sparkbasic:latest \
    --conf spark.executor.instances=1 \
    --class Main local:///opt/sparkbasics-1.0.0-jar-with-dependencies.jar
    ...
```
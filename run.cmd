@echo off 
git  branch
echo "Do you want to commit the application on the current  branch? (type  'y' or  'n'): "
@echo off 
set /p commit_response=  

if  %commit_response%==y (
  @echo off 
  echo 'Please type commit  message: 
  set /p commit_message= 
  git add  .
  git commit -m "%commit_message%"
) 

cd %cd%
rem %cd%\bin\TranCopy.exe

for /f "delims=" %%x in (.git/HEAD) do set branch=%%x
set astr=%branch%
set substr1=ref: refs/heads/
set substr2=
call set branch=%%astr:%substr1%=%substr2%%%

if NOT %branch%==master (
 echo "Do you want to merge to the local master branch? (type  'y' or  'n'):"
@echo off 
set /p commit_response=  

if  [%commit_response%]==y (

 git merge %branch%

)
)

 echo "Do you want to push to the remote master branch? (type  'y' or  'n'):"
 set /p commit_response=
if  %commit_response%==y (
    echo "checking saved remote repo..."
    IF EXIST "%cd%\remote_repo.txt" (
      set /p url= <.\remote_repo.txt
      echo "loaded repo: "+ %url%
  ) ELSE (
  echo "Please type the URL of the remote repository: "
    set /p url=
    if  [%url%] neq [] (
        echo "saving remote %url%"
      echo %url%>%cd%\remote_repo.txt
    ) )
git remote>%cd%\tmpFile
set /p remote_repo_name= <%cd%\tmpFile
del /f %cd%\tmpFile
set  bool=F
if [%remote_repo_name%]==[]  set  bool=T
if [%remote_repo_name%]==n  set  bool=T
     echo "isOriginSet: %bool%"
 if  [%bool%]==T (

   echo "git remote add origin %url%"
   git remote add origin %url%
  )
  echo "Please type the name of the branch you wish to push: " 
  set /p branch= 
  echo "Running: git push -u origin %branch% "
  git push -u origin %branch%  
  git remote 

)

pause
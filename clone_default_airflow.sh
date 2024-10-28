REPO_URL=$1
NEW_FOLDER_NAME=$2

# 기본 사용 방법 안내
if [ -z "$REPO_URL" ] || [ -z "$NEW_FOLDER_NAME" ]; then
    echo "Usage: $0 <GitHub Repository URL> <New Folder Name>"
    exit 1
fi

# 리포지토리 클론
git clone "$REPO_URL"

# 리포지토리 이름 추출 (URL에서 폴더명만 추출)
REPO_FOLDER=$(basename "$REPO_URL" .git)

# 폴더 이름 변경
mv "$REPO_FOLDER" "$NEW_FOLDER_NAME"

# 완료 메시지
echo "Repository cloned and renamed to '$NEW_FOLDER_NAME'"

# chmod +x clone_and_rename.sh
# ./clone_and_rename.sh <GitHub Repository URL> <New Folder Name>
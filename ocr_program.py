import os
import sys
from pathlib import Path
from datetime import datetime
import fitz  # PyMuPDF
from PIL import Image
import pytesseract
import argparse

class OCRProcessor:
    def __init__(self, tesseract_path=None):
        """
        OCR 프로세서 초기화
        
        Args:
            tesseract_path (str): Tesseract 실행 파일 경로 (Windows용)
        """
        if tesseract_path:
            pytesseract.pytesseract.tesseract_cmd = tesseract_path
    
    def pdf_to_images(self, pdf_path, output_dir=None):
        """
        PDF를 이미지로 변환
        
        Args:
            pdf_path (str): PDF 파일 경로
            output_dir (str): 이미지 저장 디렉토리 (기본값: None - 임시 처리)
            
        Returns:
            list: 변환된 이미지 파일 경로 리스트
        """
        try:
            # PDF 문서 열기
            doc = fitz.open(pdf_path)
            image_paths = []
            
            # 각 페이지를 이미지로 변환
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                
                # 고해상도로 렌더링 (DPI 300)
                mat = fitz.Matrix(300/72, 300/72)
                pix = page.get_pixmap(matrix=mat)
                
                # 이미지 저장
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                    img_path = os.path.join(output_dir, f"page_{page_num + 1}.png")
                else:
                    img_path = f"temp_page_{page_num + 1}.png"
                
                pix.save(img_path)
                image_paths.append(img_path)
                
                print(f"페이지 {page_num + 1} 변환 완료: {img_path}")
            
            doc.close()
            return image_paths
            
        except Exception as e:
            print(f"PDF 변환 중 오류 발생: {str(e)}")
            return []
    
    def image_to_text(self, image_path, lang='kor+eng'):
        """
        이미지를 텍스트로 변환 (OCR)
        
        Args:
            image_path (str): 이미지 파일 경로
            lang (str): OCR 언어 설정 (기본값: 한국어+영어)
            
        Returns:
            str: 추출된 텍스트
        """
        try:
            # 이미지 열기
            image = Image.open(image_path)
            
            # OCR 수행
            text = pytesseract.image_to_string(image, lang=lang)
            
            return text.strip()
            
        except Exception as e:
            print(f"OCR 처리 중 오류 발생: {str(e)}")
            return ""
    
    def process_file(self, file_path, output_file=None, lang='kor+eng', keep_images=False):
        """
        파일을 처리하여 텍스트 추출
        
        Args:
            file_path (str): 입력 파일 경로 (PDF 또는 이미지)
            output_file (str): 텍스트 저장 파일 경로 (기본값: None - 콘솔 출력)
            lang (str): OCR 언어 설정
            keep_images (bool): PDF에서 변환된 이미지 파일 보존 여부
            
        Returns:
            str: 추출된 전체 텍스트
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            print(f"파일을 찾을 수 없습니다: {file_path}")
            return ""
        
        all_text = ""
        temp_images = []
        
        try:
            # 파일 확장자 확인
            file_extension = file_path.suffix.lower()
            
            if file_extension == '.pdf':
                print("PDF 파일 감지. 이미지로 변환 중...")
                # PDF를 이미지로 변환
                temp_images = self.pdf_to_images(str(file_path))
                
                # 각 이미지에 대해 OCR 수행
                for i, img_path in enumerate(temp_images):
                    print(f"페이지 {i + 1} OCR 처리 중...")
                    text = self.image_to_text(img_path, lang)
                    if text:
                        all_text += f"\n--- 페이지 {i + 1} ---\n"
                        all_text += text + "\n"
                
            elif file_extension in ['.png', '.jpg', '.jpeg', '.tiff', '.bmp', '.gif']:
                print("이미지 파일 감지. OCR 처리 중...")
                # 이미지 파일 직접 OCR
                all_text = self.image_to_text(str(file_path), lang)
                
            else:
                print(f"지원하지 않는 파일 형식입니다: {file_extension}")
                return ""
            
            # 결과 저장 또는 출력
            if output_file:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(all_text)
                print(f"텍스트가 저장되었습니다: {output_file}")
            else:
                # 기본 파일명 생성: 원본파일명_YYYYMMDD_HHMMSS.txt
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = file_path.stem  # 확장자 제외한 파일명
                default_output = f"{base_name}_{timestamp}.txt"
                
                with open(default_output, 'w', encoding='utf-8') as f:
                    f.write(all_text)
                print(f"텍스트가 자동 저장되었습니다: {default_output}")
                
                # 콘솔에도 출력
                print("\n=== 추출된 텍스트 미리보기 ===")
                preview = all_text[:500] + "..." if len(all_text) > 500 else all_text
                print(preview)
            
            return all_text
            
        except Exception as e:
            print(f"파일 처리 중 오류 발생: {str(e)}")
            return ""
            
        finally:
            # 임시 이미지 파일 정리
            if temp_images and not keep_images:
                for img_path in temp_images:
                    try:
                        if os.path.exists(img_path) and "temp_" in img_path:
                            os.remove(img_path)
                    except:
                        pass

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='PDF/이미지 OCR 프로그램')
    parser.add_argument('input_file', help='입력 파일 경로 (PDF 또는 이미지)')
    parser.add_argument('-o', '--output', help='출력 텍스트 파일 경로')
    parser.add_argument('-l', '--lang', default='kor+eng', 
                       help='OCR 언어 설정 (기본값: kor+eng)')
    parser.add_argument('--keep-images', action='store_true',
                       help='PDF 변환 이미지 파일 보존')
    parser.add_argument('--tesseract-path', 
                       help='Tesseract 실행 파일 경로 (Windows용)')
    
    args = parser.parse_args()
    
    # OCR 프로세서 생성
    ocr = OCRProcessor(tesseract_path=args.tesseract_path)
    
    # 파일 처리
    result = ocr.process_file(
        file_path=args.input_file,
        output_file=args.output,
        lang=args.lang,
        keep_images=args.keep_images
    )
    
    if result:
        print(f"\n처리 완료! 총 {len(result)} 문자 추출됨")
    else:
        print("텍스트 추출에 실패했습니다.")

if __name__ == "__main__":
    # 직접 실행 예시
    if len(sys.argv) == 1:
        print("=== OCR 프로그램 사용 예시 ===")
        print("python ocr_program.py input.pdf")
        print("python ocr_program.py input.jpg -o output.txt")
        print("python ocr_program.py input.pdf -l eng --keep-images")
        print("\n직접 테스트하려면 파일 경로를 입력하세요:")
        
        # 간단한 대화형 모드
        file_path = input("파일 경로: ").strip()
        if file_path and os.path.exists(file_path):
            ocr = OCRProcessor()
            ocr.process_file(file_path)
        else:
            print("올바른 파일 경로를 입력해주세요.")
    else:
        main()
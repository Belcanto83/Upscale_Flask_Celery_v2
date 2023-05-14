import os
import uuid
from flask import Flask, jsonify, request, send_file
from flask.views import MethodView
from celery import Celery
from celery.result import AsyncResult

from flask_app.celery_app.celery_test_tasks import test_task
from flask_app.celery_app.upscale.upscale import upscale


FILES_DIR = os.path.join(os.getcwd(), 'celery_app', 'upscale', 'files')

app_name = 'app'
app = Flask(app_name)
app.config['UPLOAD_FOLDER'] = FILES_DIR

celery = Celery(
    app_name,
    broker='redis://localhost:6379/1',
    backend='redis://localhost:6379/2',
    # include=['flask_app.server', 'celery_app.celery_test_tasks']
)
celery.conf.update(app.config)


class FlaskTask(celery.Task):
    def __call__(self, *args, **kwargs):
        with app.app_context():
            return self.run(*args, **kwargs)


celery.Task = FlaskTask


@celery.task(name='flask_app.server.celery_task')
def celery_task(task_id):
    res = test_task(task_id)
    return res


@celery.task(name='flask_app.server.upscale_task')
def upscale_task(input_path, output_path):
    model_path = os.path.join('flask_app', 'celery_app', 'upscale', 'EDSR_x2.pb')
    in_path = os.path.join('flask_app', 'celery_app', 'upscale', 'files', input_path)
    out_path = os.path.join('flask_app', 'celery_app', 'upscale', 'files', output_path)
    upscale(in_path, out_path, model_path)
    return output_path


@app.route('/')
def set_tasks():
    task_1 = celery_task.delay(1)
    task_2 = celery_task.delay(2)

    # Это НЕ работает вместе с Flask!
    # res_1 = task_1.get()
    # res_2 = task_2.get()
    return jsonify({'res_1': task_1.id, 'res_2': task_2.id})


@app.route('/<string:task_id>')
def get_task_result(task_id):
    async_result = AsyncResult(task_id, app=celery)
    return jsonify(
        {'status': async_result.status,
         'result': async_result.result}
    )


class UpscaleView(MethodView):
    def post(self):
        image_input_path, image_output_path = self.save_file('image')
        task = upscale_task.delay(image_input_path, image_output_path)
        return jsonify({'task_id': task.id})

    @staticmethod
    def get(task_id):
        async_result = AsyncResult(task_id, app=celery)
        return jsonify(
            {'status': async_result.status,
             'result': async_result.result}
        )

    @staticmethod
    def save_file(field):
        file = request.files.get(field)
        file_extension = file.filename.split('.')[-1]
        file_id = uuid.uuid4()
        input_path = os.path.join(FILES_DIR, f'{file_id}.{file_extension}')
        # TODO: Создаем в Redis пару {file_id: file_extension}
        file.save(input_path)
        return f'{file_id}.{file_extension}', f'{file_id}-processed.{file_extension}'


@app.route('/processed/<string:file>')
def get_result_file(file):
    # TODO: Находим в Redis значение file_extension по ключу file_id
    return send_file(os.path.join(FILES_DIR, file))


upscale_view_func = UpscaleView.as_view('upscale-func')
app.add_url_rule('/upscale', view_func=upscale_view_func, methods=['POST'])
app.add_url_rule('/tasks/<string:task_id>', view_func=upscale_view_func, methods=['GET'])


if __name__ == '__main__':
    app.run(host='localhost', port=8002)

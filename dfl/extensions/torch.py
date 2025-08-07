import asyncio
import contextlib

import torch
import torch.nn as nn
from torch.optim import Optimizer
from torch.utils.data import DataLoader

import dfl.extensions.asyncio


@contextlib.asynccontextmanager
async def false_requires_grad(model: nn.Module):
	try:
		await asyncio.to_thread(model.requires_grad_, False)
		yield
	finally:
		# FIXME: preserve the original old state? Check how `torch.no_grad` works.
		await asyncio.to_thread(model.requires_grad_, True)


async def test_eval(model: nn.Module, data_loader: DataLoader, criterion=None, device=None) -> tuple[int, int] | tuple[int, int, float]:
	await asyncio.to_thread(model.eval)
	
	corrects, total = 0, 0
	if criterion is not None:
		losses_sum = 0
	
	async with false_requires_grad(model):
		async for batch, target in dfl.extensions.asyncio.iter_to_aiter(iter(data_loader)):
			if device is not None:
				batch, target = batch.to(device, non_blocking=True), target.to(device, non_blocking=True)
			
			output = await asyncio.to_thread(model, batch)
			
			# noinspection PyTypeChecker
			_, predicted = await asyncio.to_thread(torch.max, output.data, 1)
			corrects += (await asyncio.to_thread((await asyncio.to_thread(predicted.__eq__, target)).sum)).item()
			
			if criterion is not None:
				# noinspection PyUnboundLocalVariable
				losses_sum += await asyncio.to_thread(criterion, output, target) * len(target)
			
			total += len(target)
	
	if criterion is not None:
		# noinspection PyUnboundLocalVariable
		return corrects, total, float(losses_sum / total)
	else:
		return corrects, total


def train_batch(model, optimizer, criterion, batch, target, device=None):
	if device is not None:
		batch, target = batch.to(device, non_blocking=True), target.to(device, non_blocking=True)
	
	optimizer.zero_grad()
	output = model(batch)
	loss = criterion(output, target)
	loss.backward()
	optimizer.step()


async def train_epoch(model, optimizer, criterion, data_loader, device=None):
	async for batch, target in dfl.extensions.asyncio.iter_to_aiter(iter(data_loader)):
		await asyncio.to_thread(train_batch, model, optimizer, criterion, batch, target, device=device)


async def train(
		model: nn.Module,
		optimizer: Optimizer,
		criterion,
		data_loader: DataLoader,
		epochs: int | None = None,
		device=None,
):
	await asyncio.to_thread(model.train)
	
	done_epochs = 0
	while epochs is None or done_epochs < epochs:
		await train_epoch(model, optimizer, criterion, data_loader, device=device)
		done_epochs += 1


class Context:
	def __init__(
			self,
			model: nn.Module,
			optimizer: Optimizer,
			criterion,
			train_data_loader: DataLoader,
			test_data_loader: DataLoader,
			device: torch.device | None = None,
	):
		self.model = model
		self.optimizer = optimizer
		self.criterion = criterion
		self.train_data_loader = train_data_loader
		self.test_data_loader = test_data_loader
		self.device = device
	
	async def train(self, epochs: int | None = None):
		return await train(self.model, self.optimizer, self.criterion, self.train_data_loader, epochs, device=self.device)
	
	async def test_eval(self):
		return await test_eval(self.model, self.test_data_loader, criterion=self.criterion, device=self.device)
	
	async def train_eval(self):
		return await test_eval(self.model, self.train_data_loader, criterion=self.criterion, device=self.device)


def len_samples_data_loader(data_loader: DataLoader) -> int:
	total_samples = 0
	for batch in data_loader:
		total_samples += len(batch)
	return total_samples
